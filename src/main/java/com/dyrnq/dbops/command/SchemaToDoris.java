package com.dyrnq.dbops.command;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.noear.snack.ONode;
import org.noear.solon.Solon;
import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "schema-to-doris", aliases = {"std"}, description = "SchemaToDoris")
@Slf4j
public class SchemaToDoris implements Callable<Integer> {
    @CommandLine.Option(names = {"-source-ds", "--source-ds", "-S"}, description = "source datasource name", defaultValue = "default")
    String sourceDatasource;

    @CommandLine.Option(names = {"-target-ds", "--target-ds", "-T"}, description = "target datasource name", defaultValue = "default")
    String targetDatasource;

    @CommandLine.Option(names = {"-source", "--source", "--source-schema"}, description = "source schema", defaultValue = "")
    String sourceSchema;

    @CommandLine.Option(names = {"-target", "--target", "--target-schema"}, description = "target schema", defaultValue = "")
    String targetSchema;

    @CommandLine.Option(names = {"-t", "--type"}, description = "object type to convert (olap, odbc, jdbc, jdbc_catalog)", defaultValue = "olap")
    String type;

    @CommandLine.Option(names = {"-F", "--format"}, description = "output format (json, text)", defaultValue = "text")
    String format;

    @CommandLine.Option(names = {"--include-table"}, description = "regex pattern to include tables for conversion")
    String includeTablePattern;

    @CommandLine.Option(names = {"--exclude-table"}, description = "regex pattern to exclude tables from conversion")
    String excludeTablePattern;

    @CommandLine.Option(names = {"--driver-url", "--jdbc-driver-url"}, description = "", defaultValue = "https://repo.huaweicloud.com/repository/maven/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar")
    String jdbcDriverUrl;

    @CommandLine.Option(names = {"--target-table-prefix", "--table-prefix"})
    String targetTablePrefix;

    @CommandLine.Option(names = {"--target-table-suffix", "--table-suffix"})
    String targetTableSuffix;

    @CommandLine.Option(names = {"--jdbc-catalog-name", "--jdbc-catalog", "-jc"}, defaultValue = "jdbc_catalog_$sourceSchema")
    String jdbcCatalogName;

    @CommandLine.Option(names = {"--jdbc-resource-name", "--jdbc-resource", "-jr"}, defaultValue = "jdbc_resource_$sourceSchema")
    String jdbcResourceName;

    private String getJc() {
        return replaceTemplate(jdbcCatalogName);
    }

    private String getJr() {
        return replaceTemplate(jdbcResourceName);
    }

    private String replaceTemplate(String template) {
        Field[] fields = getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(CommandLine.Option.class)) {
                try {
                    field.setAccessible(true);
                    String fieldName = field.getName();
                    Object fieldValue = field.get(this);

                    if (fieldValue != null) {
                        template = template.replace("$" + fieldName, fieldValue.toString());
                    }
                } catch (IllegalAccessException e) {
                    // Handle exception
                }

            }
        }
        return template;
    }


    @Override
    public Integer call() throws Exception {
        try {
            SqlUtils sourceSqlUtils = SqlUtils.ofName(sourceDatasource);

            // If sourceSchema is empty, get the default schema from the datasource
            String sourceSchema = this.sourceSchema;
            if (sourceSchema == null || sourceSchema.isEmpty()) {
                sourceSchema = getDatabaseName(sourceSqlUtils);
                if (sourceSchema == null || sourceSchema.isEmpty()) {
                    throw new IllegalArgumentException("Source schema is not specified and cannot be determined from the datasource");
                }
            }

            // Check if schema exists
            if (!schemaExists(sourceSqlUtils, sourceSchema)) {
                throw new IllegalArgumentException("Source schema '" + sourceSchema + "' does not exist");
            }

            // Get tables from source schema
            List<Map<String, String>> sourceTables = getTables(sourceSqlUtils, sourceSchema);

            // Filter tables based on include/exclude patterns
            List<Map<String, String>> filteredTables = filterTables(sourceTables);

            log.info("Found {} tables in source schema: {}", filteredTables.size(), sourceSchema);

            List<String> sqlStatements = new ArrayList<>();

            switch (type.toLowerCase()) {
                case "olap":
                    sqlStatements = generateOlapTables(sourceSqlUtils, sourceSchema, filteredTables);
                    break;
                case "odbc":
                    sqlStatements = generateExternalOdbcTables(sourceSqlUtils, sourceSchema, filteredTables);
                    break;
                case "jdbc":
                    sqlStatements = generateExternalJdbcTables(sourceSqlUtils, sourceSchema, filteredTables);
                    break;
                case "jdbc_catalog":
                    sqlStatements = generateJdbcCatalogStatements(sourceSqlUtils, sourceSchema, filteredTables);
                    break;
                default:
                    System.err.println("Unsupported type: " + type);
                    return 1;
            }

            outputResult(sqlStatements);
            return 0;
        } catch (Exception e) {
            System.err.println("Error converting schema to Doris: " + e.getMessage());
            log.error(e.getMessage(), e);
            return 1;
        }
    }

    private List<Map<String, String>> filterTables(List<Map<String, String>> tables) {
        List<Map<String, String>> filtered = new ArrayList<>();

        Pattern includePattern = null;
        Pattern excludePattern = null;

        if (includeTablePattern != null && !includeTablePattern.isEmpty()) {
            includePattern = Pattern.compile(includeTablePattern);
        }

        if (excludeTablePattern != null && !excludeTablePattern.isEmpty()) {
            excludePattern = Pattern.compile(excludeTablePattern);
        }

        for (Map<String, String> table : tables) {
            String tableName = table.get("TABLE_NAME");

            // If include pattern is specified, table must match it
            if (includePattern != null && !includePattern.matcher(tableName).matches()) {
                continue;
            }

            // If exclude pattern is specified, table must not match it
            if (excludePattern != null && excludePattern.matcher(tableName).matches()) {
                continue;
            }

            filtered.add(table);
        }

        return filtered;
    }

    private List<String> generateOlapTables(SqlUtils sourceSqlUtils, String sourceSchema, List<Map<String, String>> tables) throws Exception {
        List<String> statements = new ArrayList<>();

        for (Map<String, String> table : tables) {
            String tableName = table.get("TABLE_NAME");
            log.info("Converting table {} to Doris OLAP table", tableName);

            // Apply table prefix and suffix if specified
            String targetTableName = targetTableName(tableName);

            String createStatement = getCreateTableStatement(sourceSqlUtils, sourceSchema, tableName);
            String dorisStatement = convertToDorisOlapTable(createStatement, tableName, targetSchema, targetTableName);
            statements.add(dorisStatement);
        }

        return statements;
    }

    private List<String> generateExternalOdbcTables(SqlUtils sourceSqlUtils, String sourceSchema, List<Map<String, String>> tables) throws Exception {
        List<String> statements = new ArrayList<>();

        // Get connection info for ODBC external table
        Map<String, String> connectionInfo = getConnectionInfo(sourceDatasource);

        for (Map<String, String> table : tables) {
            String tableName = table.get("TABLE_NAME");
            log.info("Converting table {} to Doris ODBC external table", tableName);

            // Apply table prefix and suffix if specified
            String targetTableName = targetTableName(tableName);

            String dorisTableName = StringUtils.isBlank(targetSchema) ? targetTableName : targetSchema + "." + targetTableName;

            String dorisStatement = generateOdbcExternalTable(sourceSchema, tableName, dorisTableName, connectionInfo);
            statements.add(dorisStatement);
        }

        return statements;
    }

    // Apply table prefix and suffix if specified
    private String targetTableName(String tableName) {
        return
                (StringUtils.isNoneBlank(targetTablePrefix) ? targetTablePrefix : "") +
                tableName +
                (StringUtils.isNoneBlank(targetTableSuffix) ? targetTableSuffix : "");
    }

    private List<String> generateExternalJdbcTables(SqlUtils sourceSqlUtils, String sourceSchema, List<Map<String, String>> tables) throws Exception {
        List<String> statements = new ArrayList<>();

        // Get connection info for JDBC external table
        Map<String, String> connectionInfo = getConnectionInfo(sourceDatasource);

        // For JDBC external tables, we need to create a RESOURCE first
        String resourceStatement = generateJdbcResourceStatement(connectionInfo, sourceSchema);
        statements.add(resourceStatement);

        // For each table, generate a CREATE TABLE statement that references the JDBC resource
        for (Map<String, String> table : tables) {
            String tableName = table.get("TABLE_NAME");
            log.info("Converting table {} to Doris JDBC external table", tableName);

            // Apply table prefix and suffix if specified
            String targetTableName = targetTableName(tableName);

            String dorisTableName = StringUtils.isBlank(targetSchema) ? targetTableName : targetSchema + "." + targetTableName;
            String dorisStatement = generateJdbcExternalTable(sourceSchema, tableName, dorisTableName, connectionInfo);
            statements.add(dorisStatement);
        }

        return statements;
    }

    private List<String> generateJdbcCatalogStatements(SqlUtils sourceSqlUtils, String sourceSchema, List<Map<String, String>> tables) throws Exception {
        List<String> statements = new ArrayList<>();

        // Get connection info for JDBC catalog
        Map<String, String> connectionInfo = getConnectionInfo(sourceDatasource);

        // For jdbc_catalog type, we need to create a CATALOG (not RESOURCE)
        String catalogStatement = generateJdbcCatalogStatement(connectionInfo, sourceSchema);
        statements.add(catalogStatement);

        // For jdbc_catalog, we don't create tables, just the catalog
        // But if we do need to create tables, they would reference the catalog

        return statements;
    }

    private String convertToDorisOlapTable(String mysqlCreateStatement, String mysqlTableName, String dorisSchema, String targetTableName) throws Exception {
        try {
            // Process the MySQL statement to extract CREATE TABLE part
            String createTableStatement = extractCreateTableStatement(mysqlCreateStatement);

            // Clean up unwanted parts
            createTableStatement = cleanCreateTableStatement(createTableStatement);

            // Transform table structure for Doris OLAP
            createTableStatement = transformToDorisOlap(createTableStatement);

            // Replace identifiers and add IF NOT EXISTS
            String dorisTableName = StringUtils.isBlank(dorisSchema) ? targetTableName : dorisSchema + "." + targetTableName;
            createTableStatement = replaceTableNames(createTableStatement, mysqlTableName, dorisTableName);

            // Apply type conversions
            createTableStatement = convertMysqlTypes(createTableStatement);

            // Final cleanup
            createTableStatement = finalCleanup(createTableStatement);

            return createTableStatement;
        } catch (Exception e) {
            throw new RuntimeException("Error converting MySQL table to Doris: " + e.getMessage(), e);
        }
    }

    private String extractCreateTableStatement(String input) {
        // Handle single line input by converting \n escapes to actual newlines
        if (!input.contains("\n") && input.contains("\\n")) {
            input = input.replace("\\n", "\n");
        }

        String[] lines = input.split("\n");
        List<String> outputLines = new ArrayList<>();

        boolean inCreateTable = false;
        for (String line : lines) {
            if (line.trim().startsWith("CREATE TABLE")) {
                inCreateTable = true;
                outputLines.add(line);
            } else if (inCreateTable) {
                outputLines.add(line);
                if (line.contains("ENGINE=")) {
                    break; // Stop at ENGINE line
                }
            }
        }

        return String.join("\n", outputLines);
    }

    private String cleanCreateTableStatement(String createTableStatement) {
        String[] lines = createTableStatement.split("\n");
        List<String> cleanedLines = new ArrayList<>();

        for (String line : lines) {
            // Skip constraint and key lines
            if (!line.trim().startsWith("  CON") &&
                !line.trim().startsWith("  KEY") &&
                !line.trim().startsWith("PRIMARY KEY") &&
                !line.trim().startsWith("UNIQUE KEY") &&
                !line.trim().startsWith("KEY") &&
                !line.trim().startsWith("CONSTRAINT")) {
                cleanedLines.add(line);
            }
        }

        // Remove trailing comma before closing parenthesis
        for (int i = 0; i < cleanedLines.size(); i++) {
            String line = cleanedLines.get(i);
            if (line.trim().startsWith(")")) {
                // Check previous line for trailing comma
                if (i > 0 && cleanedLines.get(i - 1).trim().endsWith(",")) {
                    String previousLine = cleanedLines.get(i - 1);
                    cleanedLines.set(i - 1, previousLine.substring(0, previousLine.lastIndexOf(",")));
                }
            }
        }

        return String.join("\n", cleanedLines);
    }

    private String transformToDorisOlap(String createTableStatement) {
        // Split the statement into lines
        String[] lines = createTableStatement.split("\n");
        List<String> transformedLines = new ArrayList<>();
        boolean engineLineFound = false;

        // Process each line, but stop when we reach the ENGINE line
        for (String line : lines) {
            if (line.contains("ENGINE=") && !engineLineFound) {
                // Add all lines up to but not including the ENGINE line
                // Then add the Doris OLAP structure
                transformedLines.add(") ENGINE=OLAP");

                // Get the first column for DUPLICATE KEY and DISTRIBUTED BY
                String firstColumn = getFirstColumn(Arrays.asList(lines));
                transformedLines.add("DUPLICATE KEY(" + firstColumn + ")");

                transformedLines.add(" COMMENT \"OLAP\"");
                transformedLines.add("DISTRIBUTED BY HASH(" + firstColumn + ") BUCKETS 10");
                transformedLines.add("PROPERTIES (");
                transformedLines.add("\"replication_allocation\" = \"tag.location.default: 3\"");
                transformedLines.add(");");
                engineLineFound = true;
//                // Don't add the ENGINE line or anything after it
//                break;
            } else if (!engineLineFound) {
                transformedLines.add(line);
            }
        }

        return String.join("\n", transformedLines);
    }

    private String getFirstColumn(List<String> lines) {
        for (String line : lines) {
            line = line.trim();
            // Skip lines that are not column definitions
            if (line.startsWith("`") && !line.startsWith(")`")) {
                // Extract column name between backticks
                int start = line.indexOf('`');
                int end = line.indexOf('`', start + 1);
                if (start >= 0 && end > start) {
                    return line.substring(start, end + 1); // Include the backticks
                }
            }
        }
        return "`id`"; // Default fallback
    }

    private String replaceTableNames(String createTableStatement, String mysqlTableName, String dorisTableName) {
        // Replace CREATE TABLE `table_name` with CREATE TABLE IF NOT EXISTS `db`.`table_name`
        String dorisTableFormatted = dorisTableName.replace(".", "`.`");
        return createTableStatement.replaceAll("TABLE `" + mysqlTableName + "`", "TABLE IF NOT EXISTS `" + dorisTableFormatted + "`");
    }

    private String convertMysqlTypes(String content) {
        // Apply all the conversions
        content = content.replaceAll("AUTO_INCREMENT", "");
        content = content.replaceAll("CHARACTER SET utf8 COLLATE utf8_bin", "");
        content = content.replaceAll("CHARACTER SET utf8mb3 COLLATE utf8mb3_bin", "");
        content = content.replaceAll("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", "");
        content = content.replaceAll("CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", "");
        content = content.replaceAll("CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", "");
        content = content.replaceAll("CHARACTER SET utf8mb4 COLLATE utf8_general_ci", "");
        content = content.replaceAll("CHARACTER SET utf8 COLLATE utf8_general_ci", "");

        // 更彻底地处理 TIMESTAMP updates - 移除 ON UPDATE CURRENT_TIMESTAMP 及其变体
        // 移除所有形式的 ON UPDATE CURRENT_TIMESTAMP，包括带参数和不带参数的
        content = content.replaceAll("(?i)\\s+ON UPDATE CURRENT_TIMESTAMP(?:\\([^)]*\\))?", "");
        // 特别处理 DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP 组合
        content = content.replaceAll("(?i)(datetime\\(\\d+\\)|datetime)\\s+DEFAULT\\s+CURRENT_TIMESTAMP(?:\\([^)]*\\))?\\s+ON UPDATE CURRENT_TIMESTAMP(?:\\([^)]*\\))?", "$1 DEFAULT CURRENT_TIMESTAMP");
        // 特别处理 DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP 组合
        content = content.replaceAll("(?i)(datetime\\(\\d+\\)|datetime)\\s+DEFAULT\\s+NULL\\s+ON UPDATE CURRENT_TIMESTAMP(?:\\([^)]*\\))?", "$1 DEFAULT NULL");

        content = content.replaceAll("CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", "");
        content = content.replaceAll("DEFAULT '0000-00-00 00:00:00'", "DEFAULT '2000-01-01 00:00:00'");

        // Handle DEFAULT CURRENT_TIMESTAMP - only for datetime columns
        content = content.replaceAll("(?i)(datetime\\(\\d+\\)|datetime)\\s+DEFAULT\\s+CURRENT_TIMESTAMP(?:\\([^)]*\\))?", "$1 DEFAULT CURRENT_TIMESTAMP");

        content = content.replaceAll("DEFAULT b", "DEFAULT");
        content = content.replaceAll("DEFAULT (\\-?[0-9]+(\\.[0-9]+)?)", "DEFAULT '$1'");
        content = content.replaceAll("CHARACTER SET utf8mb4", "");
        content = content.replaceAll("CHARACTER SET utf8", "");
        content = content.replaceAll("COLLATE utf8mb4_general_ci", "");
        content = content.replaceAll("COLLATE utf8_general_ci", "");
        content = content.replaceAll("COLLATE utf8mb4_unicode_ci", "");
        content = content.replaceAll("COLLATE utf8_unicode_ci", "");
        content = content.replaceAll("COLLATE utf8_bin", "");
        // 添加对单独 COLLATE 子句的处理
        content = content.replaceAll("(?i)\\s+COLLATE\\s+[^\\s,)]+", "");

        content = content.replaceAll("\\btinytext\\b", "varchar(65533)");
        content = content.replaceAll("text\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("\\btext\\b", "varchar(65533)");
        content = content.replaceAll("\\bmediumtext\\b", "varchar(65533)");
        content = content.replaceAll("\\blongtext\\b", "varchar(65533)");
        content = content.replaceAll("\\btinyblob\\b", "varchar(65533)");
        content = content.replaceAll("blob\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("\\bblob\\b", "varchar(65533)");
        content = content.replaceAll("\\bmediumblob\\b", "varchar(65533)");
        content = content.replaceAll("\\blongblob\\b", "varchar(65533)");
        content = content.replaceAll("\\btinystring\\b", "varchar(65533)");
        content = content.replaceAll("\\bmediumstring\\b", "varchar(65533)");
        content = content.replaceAll("\\blongstring\\b", "varchar(65533)");
        content = content.replaceAll("\\btimestamp\\b", "datetime");
        content = content.replaceAll("\\bunsigned\\b", "");
        content = content.replaceAll("\\bzerofill\\b", "");
        content = content.replaceAll("\\bjson\\b", "varchar(65533)");
        content = content.replaceAll("enum\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("set\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("\\bset\\b", "varchar(65533)");
        content = content.replaceAll("bit\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("bit\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("\\bbit\\b", "varchar(65533)");
        content = content.replaceAll("varbinary\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("binary\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("string\\([^)]*\\)", "varchar(65533)");
        content = content.replaceAll("\\bstring\\b", "varchar(65533)");
        content = content.replaceAll("\\bbinary\\b", "varchar(65533)");
        content = content.replaceAll("\\bvarbinary\\b", "varchar(65533)");
        content = content.replaceAll("\\bmediumint", "int");
        content = content.replaceAll("float\\([^)]*\\)", "float");
        content = content.replaceAll("double\\([^)]*\\)", "double");
        content = content.replaceAll("\\btime\\([^)]*\\)", "varchar(64)");
        content = content.replaceAll("\\btime\\b", "varchar(64)");
        content = content.replaceAll("year\\([^)]*\\)", "varchar(64)");
        content = content.replaceAll("\\byear\\b", "varchar(64)");

        // Fix the NULL (3) issue and datetime(n) syntax
        content = content.replaceAll("NULL \\(\\d+\\)", "NULL");
        content = content.replaceAll("datetime\\((\\d+)\\)\\s+NULL\\s*\\(\\d+\\)", "datetime($1) NULL");
        content = content.replaceAll("datetime\\((\\d+)\\)\\s+NOT NULL\\s*\\(\\d+\\)", "datetime($1) NOT NULL");

        // Fix datetime(6) (6) syntax
        content = content.replaceAll("datetime\\((\\d+)\\)\\s*\\(\\d+\\)", "datetime($1)");
        content = content.replaceAll("CURRENT_TIMESTAMP\\((\\d+)\\)\\s*\\(\\d+\\)", "CURRENT_TIMESTAMP($1)");

        // Additional cleanup for common patterns
        content = content.replaceAll(" {2,}", " "); // Replace multiple spaces with single space
        content = content.replaceAll(" DEFAULT NULL DEFAULT", " DEFAULT"); // Fix double DEFAULT
        content = content.replaceAll("  ", " "); // Fix double spaces again after replacements

        return content;
    }

    private String finalCleanup(String content) {
        // Remove trailing spaces from each line
        String[] lines = content.split("\n");
        List<String> cleanedLines = new ArrayList<>();

        for (String line : lines) {
            // Trim trailing whitespace
            line = line.replaceAll("\\s+$", "");
            cleanedLines.add(line);
        }

        // Join lines and fix any remaining issues
        String result = String.join("\n", cleanedLines);

        // Fix any remaining double spaces
        result = result.replaceAll("  ", " ");

        // Ensure there's a newline at the end
        if (!result.endsWith("\n")) {
            result += "\n";
        }

        return result;
    }

    private String generateOdbcExternalTable(String sourceSchema, String sourceTable, String dorisTable, Map<String, String> connectionInfo) {
        StringBuilder sb = new StringBuilder();

        // Get connection details
        String host = connectionInfo.get("host");
        String port = connectionInfo.get("port");
        String user = connectionInfo.get("user");
        String password = connectionInfo.get("password");
        String driver = "MySQL"; // Default driver name

        sb.append("CREATE TABLE IF NOT EXISTS `").append(dorisTable.replace(".", "`.`")).append("` (\n");

        // Add columns based on actual table schema
        try {
            List<Map<String, String>> columns = getColumns(SqlUtils.ofName(sourceDatasource), sourceSchema, sourceTable);
            for (int i = 0; i < columns.size(); i++) {
                Map<String, String> column = columns.get(i);
                String columnName = column.get("COLUMN_NAME");
                String columnType = convertMysqlTypeToDoris(column.get("DATA_TYPE"), column.get("COLUMN_TYPE"));

                sb.append("  `").append(columnName).append("` ").append(columnType);

                // Add NULL/NOT NULL
                if ("NO".equals(column.get("IS_NULLABLE"))) {
                    sb.append(" NOT NULL");
                } else {
                    sb.append(" NULL");
                }

                // Add default value
                String defaultValue = column.get("COLUMN_DEFAULT");
                if (defaultValue != null && !defaultValue.isEmpty() && !"NULL".equals(defaultValue)) {
                    if (defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                        sb.append(" DEFAULT ").append(defaultValue);
                    } else if (defaultValue.matches("-?\\d+(\\.\\d+)?")) {
                        sb.append(" DEFAULT ").append(defaultValue);
                    } else if (defaultValue.equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                        sb.append(" DEFAULT ").append(defaultValue);
                    } else {
                        sb.append(" DEFAULT '").append(defaultValue).append("'");
                    }
                }

                // Add comment
                String comment = column.get("COLUMN_COMMENT");
                if (comment != null && !comment.isEmpty()) {
                    sb.append(" COMMENT '").append(comment.replace("'", "''")).append("'");
                }

                // Add comma if not last column
                if (i < columns.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
        } catch (Exception e) {
            log.error("Error getting columns for table {}:{} {}", sourceSchema, sourceTable, e.getMessage());
            // Add a placeholder column if we can't get the actual columns
            sb.append("  `id` int NULL COMMENT 'Placeholder column'\n");
        }

        sb.append(") ENGINE=ODBC\n");
        sb.append(" COMMENT \"ODBC\"\n");
        sb.append("PROPERTIES (\n");
        sb.append("\"host\" = \"").append(host).append("\",\n");
        sb.append("\"port\" = \"").append(port).append("\",\n");
        sb.append("\"user\" = \"").append(user).append("\",\n");
        sb.append("\"password\" = \"").append(password).append("\",\n");
        sb.append("\"database\" = \"").append(sourceSchema).append("\",\n");
        sb.append("\"table\" = \"").append(sourceTable).append("\",\n");
        sb.append("\"driver\" = \"").append(driver).append("\",\n");
        sb.append("\"odbc_type\" = \"mysql\"\n");
        sb.append(");\n");

        return sb.toString();
    }

    private String generateJdbcResourceStatement(Map<String, String> connectionInfo, String sourceSchema) {
        StringBuilder sb = new StringBuilder();

        // Get connection details
        String user = connectionInfo.get("user");
        String password = connectionInfo.get("password");
        String jdbcUrl = connectionInfo.get("jdbcUrl");
        String driverUrl = this.jdbcDriverUrl;
        String driverClass = "com.mysql.cj.jdbc.Driver";
        String resourceName = getJr(); // Default resource name

        // If sourceSchema is different from the database in jdbcUrl, replace it
        if (sourceSchema != null && !sourceSchema.isEmpty() && jdbcUrl != null && jdbcUrl.contains("/")) {
            String[] parts = jdbcUrl.split("/");
            if (parts.length > 0) {
                String dbPart = parts[parts.length - 1];
                String currentDb = dbPart;
                if (dbPart.contains("?")) {
                    currentDb = dbPart.substring(0, dbPart.indexOf("?"));
                }
                if (!currentDb.equals(sourceSchema)) {
                    // Replace the database part in jdbcUrl
                    String newJdbcUrl = jdbcUrl.replace("/" + dbPart, "/" + sourceSchema + (dbPart.contains("?") ? dbPart.substring(dbPart.indexOf("?")) : ""));
                    jdbcUrl = newJdbcUrl;
                }
            }
        }

        sb.append("CREATE RESOURCE IF NOT EXISTS `").append(resourceName).append("`\n");
        sb.append("PROPERTIES (\n");
        sb.append("  \"type\"=\"jdbc\",\n");
        sb.append("  \"user\"=\"").append(user).append("\",\n");
        sb.append("  \"password\"=\"").append(password).append("\",\n");
        sb.append("  \"jdbc_url\"=\"").append(jdbcUrl).append("\",\n");
        sb.append("  \"driver_url\"=\"").append(driverUrl).append("\",\n");
        sb.append("  \"driver_class\"=\"").append(driverClass).append("\"\n");
        sb.append(");\n");

        return sb.toString();
    }

    private String generateJdbcCatalogStatement(Map<String, String> connectionInfo, String sourceSchema) {
        StringBuilder sb = new StringBuilder();

        // Get connection details
        String user = connectionInfo.get("user");
        String password = connectionInfo.get("password");
        String jdbcUrl = connectionInfo.get("jdbcUrl");
        // Extract database name from jdbcUrl for catalog
        String databaseName = "information_schema"; // Default
        if (jdbcUrl != null && jdbcUrl.contains("/")) {
            String[] parts = jdbcUrl.split("/");
            if (parts.length > 0) {
                String dbPart = parts[parts.length - 1];
                if (dbPart.contains("?")) {
                    databaseName = dbPart.substring(0, dbPart.indexOf("?"));
                } else {
                    databaseName = dbPart;
                }
            }
        }
        String driverUrl = this.jdbcDriverUrl;
        String driverClass = "com.mysql.cj.jdbc.Driver";
        String catalogName = getJc(); // Default catalog name

        // If sourceSchema is different from the database in jdbcUrl, replace it
        if (sourceSchema != null && !sourceSchema.isEmpty() && jdbcUrl != null && jdbcUrl.contains("/")) {
            String[] parts = jdbcUrl.split("/");
            if (parts.length > 0) {
                String dbPart = parts[parts.length - 1];
                String currentDb = dbPart;
                if (dbPart.contains("?")) {
                    currentDb = dbPart.substring(0, dbPart.indexOf("?"));
                }
                if (!currentDb.equals(sourceSchema)) {
                    // Replace the database part in jdbcUrl
                    String newJdbcUrl = jdbcUrl.replace("/" + dbPart, "/" + sourceSchema + (dbPart.contains("?") ? dbPart.substring(dbPart.indexOf("?")) : ""));
                    jdbcUrl = newJdbcUrl;
                }
            }
        }

        sb.append("CREATE CATALOG IF NOT EXISTS `").append(catalogName).append("`\n");
        sb.append("PROPERTIES (\n");
        sb.append("  \"type\"=\"jdbc\",\n");
        sb.append("  \"user\"=\"").append(user).append("\",\n");
        sb.append("  \"password\"=\"").append(password).append("\",\n");
        sb.append("  \"jdbc_url\"=\"").append(jdbcUrl).append("\",\n");
        sb.append("  \"driver_url\"=\"").append(driverUrl).append("\",\n");
        sb.append("  \"driver_class\"=\"").append(driverClass).append("\"\n");
        sb.append(");\n");

        return sb.toString();
    }

    private String generateJdbcExternalTable(String sourceSchema, String sourceTable, String dorisTable, Map<String, String> connectionInfo) {
        StringBuilder sb = new StringBuilder();

        String dorisTableFormatted = dorisTable.replace(".", "`.`");
        sb.append("CREATE TABLE IF NOT EXISTS `").append(dorisTableFormatted).append("` (\n");

        // Add columns based on actual table schema
        try {
            List<Map<String, String>> columns = getColumns(SqlUtils.ofName(sourceDatasource), sourceSchema, sourceTable);
            for (int i = 0; i < columns.size(); i++) {
                Map<String, String> column = columns.get(i);
                String columnName = column.get("COLUMN_NAME");
                String columnType = convertMysqlTypeToDoris(column.get("DATA_TYPE"), column.get("COLUMN_TYPE"));

                sb.append("  `").append(columnName).append("` ").append(columnType);

                // Add NULL/NOT NULL
                if ("NO".equals(column.get("IS_NULLABLE"))) {
                    sb.append(" NOT NULL");
                } else {
                    sb.append(" NULL");
                }

                // Add default value
                String defaultValue = column.get("COLUMN_DEFAULT");
                if (defaultValue != null && !defaultValue.isEmpty() && !"NULL".equals(defaultValue)) {
                    if (defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                        sb.append(" DEFAULT ").append(defaultValue);
                    } else if (defaultValue.matches("-?\\d+(\\.\\d+)?")) {
                        sb.append(" DEFAULT ").append(defaultValue);
                    } else if (defaultValue.equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                        sb.append(" DEFAULT ").append(defaultValue);
                    } else {
                        sb.append(" DEFAULT '").append(defaultValue).append("'");
                    }
                }

                // Add comment
                String comment = column.get("COLUMN_COMMENT");
                if (comment != null && !comment.isEmpty()) {
                    sb.append(" COMMENT '").append(comment.replace("'", "''")).append("'");
                }

                // Add comma if not last column
                if (i < columns.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
        } catch (Exception e) {
            log.error("Error getting columns for table {}:{} {}", sourceSchema, sourceTable, e.getMessage());
            // Add a placeholder column if we can't get the actual columns
            sb.append("  `id` int NULL COMMENT 'Placeholder column'\n");
        }

        sb.append(") ENGINE=JDBC\n");
        sb.append(" COMMENT \"JDBC\"\n");
        sb.append("PROPERTIES (\n");
        sb.append("\"resource\" = \"jdbc_catalog\",\n");
        sb.append("\"database\" = \"").append(sourceSchema).append("\",\n");
        sb.append("\"table\" = \"").append(sourceTable).append("\",\n");

        // Add table_type property based on jdbc_url
        String jdbcUrl = connectionInfo.get("jdbcUrl");
        String tableType = "mysql"; // default
        if (jdbcUrl != null) {
            if (jdbcUrl.startsWith("jdbc:mysql:")) {
                tableType = "mysql";
            } else if (jdbcUrl.startsWith("jdbc:postgresql:")) {
                tableType = "postgresql";
            } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
                tableType = "oracle";
            } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
                tableType = "sqlserver";
            }
        }
        sb.append("\"table_type\" = \"").append(tableType).append("\"\n"); // Add table_type property
        sb.append(");\n");

        return sb.toString();
    }

    // This method is not needed for jdbc_catalog type, as we only create the catalog, not the tables

    private String convertMysqlTypeToDoris(String dataType, String columnType) {
        // Handle specific column types first
        if (columnType != null && !columnType.isEmpty()) {
            // Handle VARCHAR with length
            if (columnType.toLowerCase().startsWith("varchar")) {
                return columnType.replaceAll("varchar\\((\\d+)\\)", "varchar($1)");
            }
            // Handle CHAR with length
            if (columnType.toLowerCase().startsWith("char")) {
                return columnType.replaceAll("char\\((\\d+)\\)", "char($1)");
            }
            // Handle DATETIME with precision
            if (columnType.toLowerCase().startsWith("datetime")) {
                return columnType.replaceAll("datetime\\((\\d+)\\)", "datetime($1)");
            }
        }

        // Handle data types
        return switch (dataType.toLowerCase()) {
            case "tinyint" -> "tinyint";
            case "smallint" -> "smallint";
            case "mediumint", "int" -> "int";
            case "bigint" -> "bigint";
            case "float" -> "float";
            case "double" -> "double";
            case "decimal" -> {
                // Extract precision and scale if available
                if (columnType != null && columnType.matches("decimal\\(\\d+,\\d+\\)")) {
                    yield columnType.replaceAll("decimal", "decimal");
                }
                yield "decimal(10,2)";
            }
            case "date" -> "date";
            case "time", "year" -> "varchar(64)";
            case "datetime", "timestamp" -> "datetime";
            case "char" -> "char(1)";
            case "varchar" -> "varchar(255)";
//            case "tinytext", "text", "mediumtext", "longtext" -> "varchar(65533)";
//            case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob" -> "varchar(65533)";
//            case "enum", "set" -> "varchar(65533)";
//            case "json" -> "varchar(65533)";
            default -> "varchar(65533)";
        };
    }

    private Map<String, String> getConnectionInfo(String datasourceName) throws SQLException {
        Map<String, String> connectionInfo = new HashMap<>();

        // Get the datasource from Solon context
        DataSource dataSource = Solon.context().getBean(datasourceName);

        if (dataSource != null) {
            try (Connection conn = dataSource.getConnection()) {
                DatabaseMetaData metaData = conn.getMetaData();
                String url = metaData.getURL();
                // Parse URL like jdbc:mysql://host:port/database
                if (url.startsWith("jdbc:mysql://")) {
                    String[] parts = url.substring(13).split("/");
                    String hostPort = parts[0];
                    String[] hostPortParts = hostPort.split(":");
                    connectionInfo.put("host", hostPortParts[0]);
                    connectionInfo.put("port", hostPortParts.length > 1 ? hostPortParts[1] : "3306");
                    connectionInfo.put("jdbcUrl", url);
                }
//                connectionInfo.put("user", metaData.getUserName());
            }
            if (dataSource instanceof HikariDataSource hikariDataSource) {
                connectionInfo.put("password", hikariDataSource.getPassword());
                connectionInfo.put("user", hikariDataSource.getUsername());
            }
        }

        return connectionInfo;
    }

    private void outputResult(List<String> sqlStatements) {
        if ("text".equals(format)) {
            for (String statement : sqlStatements) {
                System.out.println(statement);
            }
        } else {
            // JSON format - return array of SQL statements directly
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(sqlStatements));
        }
    }

    private List<Map<String, String>> getTables(SqlUtils sqlUtils, String schema) throws Exception {
        String sql = "SELECT TABLE_NAME, TABLE_SCHEMA FROM information_schema.TABLES";
        if (schema != null && !schema.isEmpty()) {
            sql += " WHERE TABLE_SCHEMA = '" + schema + "'";
        } else {
            sql += " WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')";
        }
        sql += " AND TABLE_TYPE = 'BASE TABLE'";

        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        return parseResult(result);
    }

    private List<Map<String, String>> getColumns(SqlUtils sqlUtils, String schema, String table) throws Exception {
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT " +
                     "FROM information_schema.COLUMNS " +
                     "WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = '" + table + "' " +
                     "ORDER BY ORDINAL_POSITION";

        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        return parseResult(result);
    }

    private String getCreateTableStatement(SqlUtils sqlUtils, String schema, String tableName) throws Exception {
        String sql = "SHOW CREATE TABLE `" + schema + "`.`" + tableName + "`";
        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        if (result != null && !result.isEmpty()) {
            ONode o = ONode.loadStr(result.get(0));
            return o.get("Create Table").getString();
        }
        return "-- Could not retrieve CREATE statement for " + tableName;
    }

    private List<Map<String, String>> parseResult(List<String> result) {
        List<Map<String, String>> parsed = new ArrayList<>();
        if (result == null || result.isEmpty()) {
            return parsed;
        }
        for (String row : result) {
            if (row != null && !row.trim().isEmpty()) {
                try {
                    ONode o = ONode.loadStr(row);
                    Map<String, String> map = new HashMap<>();
                    // 使用 forEach 遍历 ONode 对象的所有键值对
                    o.forEach((k, v) -> {
                        map.put(k, v.getString());
                    });
                    parsed.add(map);
                } catch (Exception e) {
                    System.err.println("Error parsing row: " + row + ", error: " + e.getMessage());
                }
            }
        }
        return parsed;
    }

    private String getDatabaseName(SqlUtils sqlUtils) throws Exception {
        try {
            // For MySQL, we can get the current database name
            List<String> result = sqlUtils.sql("SELECT DATABASE()").queryRowList(String.class);
            if (result != null && !result.isEmpty() && result.get(0) != null) {
                // Parse the JSON result to extract the database name
                String jsonResult = result.get(0);
                ONode o = ONode.loadStr(jsonResult);
                return o.get("DATABASE()").getString();
            }
        } catch (Exception e) {
            // If DATABASE() fails, try another approach
            log.warn("Could not determine database name: " + e.getMessage());
        }
        return null;
    }

    private boolean schemaExists(SqlUtils sqlUtils, String schema) throws Exception {
        String sql = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '" + schema + "'";
        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        return result != null && !result.isEmpty();
    }
}