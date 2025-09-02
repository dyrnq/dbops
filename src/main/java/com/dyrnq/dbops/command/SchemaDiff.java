package com.dyrnq.dbops.command;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.noear.snack.ONode;
import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "schema-diff", aliases = {"sd"}, description = "SchemaDiff")
@Slf4j
public class SchemaDiff implements Callable<Integer> {
    @CommandLine.Option(names = {"-source-ds", "--source-ds", "-S"}, description = "source datasource name", defaultValue = "default")
    String sourceDatasource;
    @CommandLine.Option(names = {"-target-ds", "--target-ds", "-T"}, description = "target datasource name", defaultValue = "default")
    String targetDatasource;

    @CommandLine.Option(names = {"-source", "--source", "--source-schema"}, description = "source schema", defaultValue = "")
    String sourceSchema;
    @CommandLine.Option(names = {"-target", "--target", "--target-schema"}, description = "target schema", defaultValue = "")
    String targetSchema;

    @CommandLine.Option(names = {"-t", "--type"}, description = "object type to compare (table, view, procedure, function, trigger, event)", defaultValue = "table")
    String type;

    @CommandLine.Option(names = {"-F", "--format"}, description = "output format (json, text)", defaultValue = "text")
    String format;

    @CommandLine.Option(names = {"-d", "--drop"}, description = "include drop statements for objects only in target", defaultValue = "false")
    boolean includeDrop;

    @Override
    public Integer call() throws Exception {
        try {
            SqlUtils sourceSqlUtils = SqlUtils.ofName(sourceDatasource);
            SqlUtils targetSqlUtils = SqlUtils.ofName(targetDatasource);

            // If sourceSchema or targetSchema is empty, get the default schema from the datasource
            String sourceSchema = this.sourceSchema;
            String targetSchema = this.targetSchema;

            if (sourceSchema == null || sourceSchema.isEmpty()) {
                sourceSchema = getDatabaseName(sourceSqlUtils);
                if (sourceSchema == null || sourceSchema.isEmpty()) {
                    throw new IllegalArgumentException("Source schema is not specified and cannot be determined from the datasource");
                }
            }

            if (targetSchema == null || targetSchema.isEmpty()) {
                targetSchema = getDatabaseName(targetSqlUtils);
                if (targetSchema == null || targetSchema.isEmpty()) {
                    throw new IllegalArgumentException("Target schema is not specified and cannot be determined from the datasource");
                }
            }

            // Check if schemas exist
            if (!schemaExists(sourceSqlUtils, sourceSchema)) {
                throw new IllegalArgumentException("Source schema '" + sourceSchema + "' does not exist");
            }

            if (!schemaExists(targetSqlUtils, targetSchema)) {
                throw new IllegalArgumentException("Target schema '" + targetSchema + "' does not exist");
            }

            switch (type.toLowerCase()) {
                case "table":
                    compareTables(sourceSqlUtils, targetSqlUtils, sourceSchema, targetSchema);
                    break;
                case "view":
                    compareViews(sourceSqlUtils, targetSqlUtils, sourceSchema, targetSchema);
                    break;
                case "procedure":
                case "function":
                    compareRoutines(sourceSqlUtils, targetSqlUtils, sourceSchema, targetSchema);
                    break;
                case "trigger":
                    compareTriggers(sourceSqlUtils, targetSqlUtils, sourceSchema, targetSchema);
                    break;
                case "event":
                    compareEvents(sourceSqlUtils, targetSqlUtils, sourceSchema, targetSchema);
                    break;
                default:
                    System.err.println("Unsupported object type: " + type);
                    return 1;
            }

            return 0;
        } catch (Exception e) {
            System.err.println("Error comparing schemas: " + e.getMessage());
            log.error(e.getMessage(), e);
            return 1;
        }
    }

    private void compareTables(SqlUtils sourceSqlUtils, SqlUtils targetSqlUtils, String sourceSchema, String targetSchema) throws Exception {
        log.info("Comparing tables between source schema: {} and target schema: {}", sourceSchema, targetSchema);

        // Get tables from source
        List<Map<String, String>> sourceTables = getTables(sourceSqlUtils, sourceSchema);
        Set<String> sourceTableNames = sourceTables.stream()
                .map(table -> table.get("TABLE_NAME"))
                .collect(Collectors.toSet());

        log.info("Source tables count: {}", sourceTableNames.size());

        // Get tables from target
        List<Map<String, String>> targetTables = getTables(targetSqlUtils, targetSchema);
        Set<String> targetTableNames = targetTables.stream()
                .map(table -> table.get("TABLE_NAME"))
                .collect(Collectors.toSet());

        log.info("Target tables count: {}", targetTableNames.size());

        // Find differences
        Set<String> onlyInSource = new HashSet<>(sourceTableNames);
        onlyInSource.removeAll(targetTableNames);

        Set<String> onlyInTarget = new HashSet<>(targetTableNames);
        onlyInTarget.removeAll(sourceTableNames);

        Set<String> inBoth = new HashSet<>(sourceTableNames);
        inBoth.retainAll(targetTableNames);

        log.info("Tables only in source count: {}", onlyInSource.size());
        log.info("Tables only in target count: {}", onlyInTarget.size());
        log.info("Tables in both count: {}", inBoth.size());

        // For JSON format, collect all SQL statements in a single list
        List<String> allSqlStatements = new ArrayList<>();

        // Output results in text format
        if ("text".equals(format)) {
            // Handle tables only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                log.info("=== Tables only in source ({}) ===", sourceSchema);
                for (String tableName : onlyInSource) {
                    String createStatement = getCreateTableStatement(sourceSqlUtils, sourceSchema, tableName);
                    System.out.println(createStatement + ";");
                    System.out.println();
                }
            }

            // Handle tables only in target - generate DROP statements if enabled
            if (!onlyInTarget.isEmpty()) {
                log.info("=== Tables only in target ({}) ===", targetSchema);
                if (includeDrop) {
                    for (String tableName : onlyInTarget) {
                        System.out.println("DROP TABLE `" + tableName + "`;");
                    }
                } else {
                    log.info("(Drop statements not included. Use --drop to include them.)");
                }
                System.out.println();
            }

            // For tables in both, compare columns and generate ALTER statements
            if (!inBoth.isEmpty()) {
                log.info("Found {} tables in both schemas", inBoth.size());
                boolean hasDifferences = false;
                for (String tableName : inBoth) {
                    List<Map<String, String>> sourceColumns = getColumns(sourceSqlUtils, sourceSchema, tableName);
                    List<Map<String, String>> targetColumns = getColumns(targetSqlUtils, targetSchema, tableName);

                    // Create maps with column name as key for easier comparison
                    Map<String, Map<String, String>> sourceColumnMap = new HashMap<>();
                    Map<String, Map<String, String>> targetColumnMap = new HashMap<>();

                    for (Map<String, String> col : sourceColumns) {
                        sourceColumnMap.put(col.get("COLUMN_NAME"), col);
                    }

                    for (Map<String, String> col : targetColumns) {
                        targetColumnMap.put(col.get("COLUMN_NAME"), col);
                    }

                    Set<String> sourceColumnNames = sourceColumnMap.keySet();
                    Set<String> targetColumnNames = targetColumnMap.keySet();

                    Set<String> colsOnlyInSource = new HashSet<>(sourceColumnNames);
                    colsOnlyInSource.removeAll(targetColumnNames);

                    Set<String> colsOnlyInTarget = new HashSet<>(targetColumnNames);
                    colsOnlyInTarget.removeAll(sourceColumnNames);

                    // Check for column differences (same column name but different properties)
                    Map<String, Object> columnPropertyDiffs = new HashMap<>();
                    Set<String> commonColumns = new HashSet<>(sourceColumnNames);
                    commonColumns.retainAll(targetColumnNames);

                    for (String columnName : commonColumns) {
                        Map<String, String> sourceCol = sourceColumnMap.get(columnName);
                        Map<String, String> targetCol = targetColumnMap.get(columnName);

                        Map<String, Object> diffDetails = new HashMap<>();
                        boolean hasDiff = false;

                        // Compare all properties
                        String[] properties = {
                                "DATA_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT",
                                "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE",
                                "EXTRA", "COLUMN_KEY", "COLUMN_COMMENT"
                        };

                        for (String property : properties) {
                            String sourceValue = sourceCol.get(property);
                            String targetValue = targetCol.get(property);
                            if (!Objects.equals(sourceValue, targetValue)) {
                                diffDetails.put(property, Map.of("source", sourceValue, "target", targetValue));
                                hasDiff = true;
                            }
                        }

                        if (hasDiff) {
                            columnPropertyDiffs.put(columnName, diffDetails);
                        }
                    }

                    // Generate ALTER statements if there are differences
                    if (!colsOnlyInSource.isEmpty() || !colsOnlyInTarget.isEmpty() || !columnPropertyDiffs.isEmpty()) {
                        if (!hasDifferences) {
                            hasDifferences = true;
                            log.info("=== Tables in both schemas with differences ===");
                        }
                        System.out.println("-- Differences for table: " + tableName);

                        // Columns only in source - ADD COLUMN statements
                        if (!colsOnlyInSource.isEmpty()) {
                            log.info("Columns only in source schema:");
                            for (String columnName : colsOnlyInSource) {
                                Map<String, String> col = sourceColumnMap.get(columnName);
                                String columnDefinition = getColumnDefinition(col);
                                System.out.println("ALTER TABLE `" + tableName + "` ADD COLUMN " + columnDefinition + ";");
                            }
                        }

                        // Columns only in target - DROP COLUMN statements
                        if (!colsOnlyInTarget.isEmpty()) {
                            log.info("Columns only in target schema:");
                            for (String columnName : colsOnlyInTarget) {
                                System.out.println("ALTER TABLE `" + tableName + "` DROP COLUMN `" + columnName + "`;");
                            }
                        }

                        // Column property differences - MODIFY COLUMN statements
                        if (!columnPropertyDiffs.isEmpty()) {
                            log.info("Columns with property differences:");
                            for (Map.Entry<String, Object> entry : columnPropertyDiffs.entrySet()) {
                                String columnName = entry.getKey();
                                Map<String, String> sourceCol = sourceColumnMap.get(columnName);
                                String columnDefinition = getColumnDefinition(sourceCol);
                                System.out.println("ALTER TABLE `" + tableName + "` MODIFY COLUMN " + columnDefinition + ";");
                            }
                        }
                        System.out.println();
                    } else {
                        log.info("No column differences found for table: {}", tableName);
                    }
                }

                if (!hasDifferences) {
                    log.info("=== No column differences found in common tables ===");
                }
            } else {
                log.info("=== No common tables found between source and target schemas ===");
            }

            // Summary when there are no differences at all
            if (onlyInSource.isEmpty() && onlyInTarget.isEmpty() && inBoth.isEmpty()) {
                log.info("=== No tables found in either source or target schemas ===");
            }
        } else {
            // JSON format - return array of SQL statements directly
            // Handle tables only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                for (String tableName : onlyInSource) {
                    String createStatement = getCreateTableStatement(sourceSqlUtils, sourceSchema, tableName);
                    allSqlStatements.add(createStatement + ";");
                }
            }

            // Handle tables only in target - generate DROP statements if enabled
            if (!onlyInTarget.isEmpty()) {
                if (includeDrop) {
                    for (String tableName : onlyInTarget) {
                        allSqlStatements.add("DROP TABLE `" + tableName + "`;");
                    }
                }
            }

            // For tables in both, compare columns and generate ALTER statements
            if (!inBoth.isEmpty()) {
                for (String tableName : inBoth) {
                    List<Map<String, String>> sourceColumns = getColumns(sourceSqlUtils, sourceSchema, tableName);
                    List<Map<String, String>> targetColumns = getColumns(targetSqlUtils, targetSchema, tableName);

                    // Create maps with column name as key for easier comparison
                    Map<String, Map<String, String>> sourceColumnMap = new HashMap<>();
                    Map<String, Map<String, String>> targetColumnMap = new HashMap<>();

                    for (Map<String, String> col : sourceColumns) {
                        sourceColumnMap.put(col.get("COLUMN_NAME"), col);
                    }

                    for (Map<String, String> col : targetColumns) {
                        targetColumnMap.put(col.get("COLUMN_NAME"), col);
                    }

                    Set<String> sourceColumnNames = sourceColumnMap.keySet();
                    Set<String> targetColumnNames = targetColumnMap.keySet();

                    Set<String> colsOnlyInSource = new HashSet<>(sourceColumnNames);
                    colsOnlyInSource.removeAll(targetColumnNames);

                    Set<String> colsOnlyInTarget = new HashSet<>(targetColumnNames);
                    colsOnlyInTarget.removeAll(sourceColumnNames);

                    // Check for column differences (same column name but different properties)
                    Map<String, Object> columnPropertyDiffs = new HashMap<>();
                    Set<String> commonColumns = new HashSet<>(sourceColumnNames);
                    commonColumns.retainAll(targetColumnNames);

                    for (String columnName : commonColumns) {
                        Map<String, String> sourceCol = sourceColumnMap.get(columnName);
                        Map<String, String> targetCol = targetColumnMap.get(columnName);

                        Map<String, Object> diffDetails = new HashMap<>();
                        boolean hasDiff = false;

                        // Compare all properties
                        String[] properties = {
                                "DATA_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT",
                                "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE",
                                "EXTRA", "COLUMN_KEY", "COLUMN_COMMENT"
                        };

                        for (String property : properties) {
                            String sourceValue = sourceCol.get(property);
                            String targetValue = targetCol.get(property);
                            if (!Objects.equals(sourceValue, targetValue)) {
                                diffDetails.put(property, Map.of("source", sourceValue, "target", targetValue));
                                hasDiff = true;
                            }
                        }

                        if (hasDiff) {
                            columnPropertyDiffs.put(columnName, diffDetails);
                        }
                    }

                    // Generate ALTER statements if there are differences
                    List<String> tableAlterStatements = new ArrayList<>();
                    if (!colsOnlyInSource.isEmpty() || !colsOnlyInTarget.isEmpty() || !columnPropertyDiffs.isEmpty()) {
                        // Columns only in source - ADD COLUMN statements
                        if (!colsOnlyInSource.isEmpty()) {
                            for (String columnName : colsOnlyInSource) {
                                Map<String, String> col = sourceColumnMap.get(columnName);
                                String columnDefinition = getColumnDefinition(col);
                                tableAlterStatements.add("ALTER TABLE `" + tableName + "` ADD COLUMN " + columnDefinition + ";");
                            }
                        }

                        // Columns only in target - DROP COLUMN statements
                        if (!colsOnlyInTarget.isEmpty()) {
                            for (String columnName : colsOnlyInTarget) {
                                tableAlterStatements.add("ALTER TABLE `" + tableName + "` DROP COLUMN `" + columnName + "`;");
                            }
                        }

                        // Column property differences - MODIFY COLUMN statements
                        if (!columnPropertyDiffs.isEmpty()) {
                            for (Map.Entry<String, Object> entry : columnPropertyDiffs.entrySet()) {
                                String columnName = entry.getKey();
                                Map<String, String> sourceCol = sourceColumnMap.get(columnName);
                                String columnDefinition = getColumnDefinition(sourceCol);
                                tableAlterStatements.add("ALTER TABLE `" + tableName + "` MODIFY COLUMN " + columnDefinition + ";");
                            }
                        }

                        if (!tableAlterStatements.isEmpty()) {
                            allSqlStatements.addAll(tableAlterStatements);
                        }
                    }
                }
            }

            // Output JSON as array of SQL statements
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(allSqlStatements));
        }
    }

    private void compareViews(SqlUtils sourceSqlUtils, SqlUtils targetSqlUtils, String sourceSchema, String targetSchema) throws Exception {
        // Get views from source
        List<Map<String, String>> sourceViews = getViews(sourceSqlUtils, sourceSchema);
        Set<String> sourceViewNames = sourceViews.stream()
                .map(view -> view.get("TABLE_NAME"))
                .collect(Collectors.toSet());

        // Get views from target
        List<Map<String, String>> targetViews = getViews(targetSqlUtils, targetSchema);
        Set<String> targetViewNames = targetViews.stream()
                .map(view -> view.get("TABLE_NAME"))
                .collect(Collectors.toSet());

        // Find differences
        Set<String> onlyInSource = new HashSet<>(sourceViewNames);
        onlyInSource.removeAll(targetViewNames);

        Set<String> onlyInTarget = new HashSet<>(targetViewNames);
        onlyInTarget.removeAll(sourceViewNames);

        // For JSON format, collect all SQL statements in a single list
        List<String> allSqlStatements = new ArrayList<>();

        // Output results in text format
        if ("text".equals(format)) {
            // Handle views only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                log.info("=== Views only in source ({}) ===", sourceSchema);
                for (String viewName : onlyInSource) {
                    String createStatement = getCreateViewStatement(sourceSqlUtils, sourceSchema, viewName);
                    System.out.println(createStatement + ";");
                    System.out.println();
                }
            }

            // Handle views only in target - generate DROP statements
            if (!onlyInTarget.isEmpty()) {
                log.info("=== Views only in target ({}) ===", targetSchema);
                if (includeDrop) {
                    for (String viewName : onlyInTarget) {
                        System.out.println("DROP VIEW `" + viewName + "`;");
                    }
                } else {
                    log.info("(Drop statements not included. Use --drop to include them.)");
                }
                System.out.println();
            }

            if (onlyInSource.isEmpty() && onlyInTarget.isEmpty()) {
                log.info("=== No view differences found between source and target schemas ===");
            }
        } else {
            // JSON format - return array of SQL statements directly
            // Handle views only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                for (String viewName : onlyInSource) {
                    String createStatement = getCreateViewStatement(sourceSqlUtils, sourceSchema, viewName);
                    allSqlStatements.add(createStatement + ";");
                }
            }

            // Handle views only in target - generate DROP statements
            if (!onlyInTarget.isEmpty()) {
                if (includeDrop) {
                    for (String viewName : onlyInTarget) {
                        allSqlStatements.add("DROP VIEW `" + viewName + "`;");
                    }
                }
            }

            // Output JSON as array of SQL statements
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(allSqlStatements));
        }
    }

    private void compareRoutines(SqlUtils sourceSqlUtils, SqlUtils targetSqlUtils, String sourceSchema, String targetSchema) throws Exception {
        // Get routines from source
        List<Map<String, String>> sourceRoutines = getRoutines(sourceSqlUtils, sourceSchema, type);
        Set<String> sourceRoutineNames = sourceRoutines.stream()
                .map(routine -> routine.get("ROUTINE_NAME"))
                .collect(Collectors.toSet());

        // Get routines from target
        List<Map<String, String>> targetRoutines = getRoutines(targetSqlUtils, targetSchema, type);
        Set<String> targetRoutineNames = targetRoutines.stream()
                .map(routine -> routine.get("ROUTINE_NAME"))
                .collect(Collectors.toSet());

        // Find differences
        Set<String> onlyInSource = new HashSet<>(sourceRoutineNames);
        onlyInSource.removeAll(targetRoutineNames);

        Set<String> onlyInTarget = new HashSet<>(targetRoutineNames);
        onlyInTarget.removeAll(sourceRoutineNames);

        // For JSON format, collect all SQL statements in a single list
        List<String> allSqlStatements = new ArrayList<>();

        // Output results in text format
        if ("text".equals(format)) {
            // Handle routines only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                log.info("=== {}s only in source ({}) ===", type, sourceSchema);
                for (String routineName : onlyInSource) {
                    String createStatement = getCreateRoutineStatement(sourceSqlUtils, sourceSchema, routineName, type);
                    System.out.println(createStatement + ";");
                    System.out.println();
                }
            }

            // Handle routines only in target - generate DROP statements
            if (!onlyInTarget.isEmpty()) {
                log.info("=== {}s only in target ({}) ===", type, targetSchema);
                if (includeDrop) {
                    for (String routineName : onlyInTarget) {
                        System.out.println("DROP " + type.toUpperCase() + " `" + routineName + "`;");
                    }
                } else {
                    log.info("(Drop statements not included. Use --drop to include them.)");
                }
                System.out.println();
            }

            if (onlyInSource.isEmpty() && onlyInTarget.isEmpty()) {
                log.info("=== No {} differences found between source and target schemas ===", type);
            }
        } else {
            // JSON format - return array of SQL statements directly
            // Handle routines only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                for (String routineName : onlyInSource) {
                    String createStatement = getCreateRoutineStatement(sourceSqlUtils, sourceSchema, routineName, type);
                    allSqlStatements.add(createStatement + ";");
                }
            }

            // Handle routines only in target - generate DROP statements
            if (!onlyInTarget.isEmpty()) {
                if (includeDrop) {
                    for (String routineName : onlyInTarget) {
                        allSqlStatements.add("DROP " + type.toUpperCase() + " `" + routineName + "`;");
                    }
                }
            }

            // Output JSON as array of SQL statements
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(allSqlStatements));
        }
    }

    private void compareTriggers(SqlUtils sourceSqlUtils, SqlUtils targetSqlUtils, String sourceSchema, String targetSchema) throws Exception {
        // Get triggers from source
        List<Map<String, String>> sourceTriggers = getTriggers(sourceSqlUtils, sourceSchema);
        Set<String> sourceTriggerNames = sourceTriggers.stream()
                .map(trigger -> trigger.get("TRIGGER_NAME"))
                .collect(Collectors.toSet());

        // Get triggers from target
        List<Map<String, String>> targetTriggers = getTriggers(targetSqlUtils, targetSchema);
        Set<String> targetTriggerNames = targetTriggers.stream()
                .map(trigger -> trigger.get("TRIGGER_NAME"))
                .collect(Collectors.toSet());

        // Find differences
        Set<String> onlyInSource = new HashSet<>(sourceTriggerNames);
        onlyInSource.removeAll(targetTriggerNames);

        Set<String> onlyInTarget = new HashSet<>(targetTriggerNames);
        onlyInTarget.removeAll(sourceTriggerNames);

        // For JSON format, collect all SQL statements in a single list
        List<String> allSqlStatements = new ArrayList<>();

        // Output results in text format
        if ("text".equals(format)) {
            // Handle triggers only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                log.info("=== Triggers only in source ({}) ===", sourceSchema);
                for (String triggerName : onlyInSource) {
                    String createStatement = getCreateTriggerStatement(sourceSqlUtils, sourceSchema, triggerName);
                    System.out.println(createStatement + ";");
                    System.out.println();
                }
            }

            // Handle triggers only in target - generate DROP statements
            if (!onlyInTarget.isEmpty()) {
                log.info("=== Triggers only in target ({}) ===", targetSchema);
                if (includeDrop) {
                    for (String triggerName : onlyInTarget) {
                        System.out.println("DROP TRIGGER `" + triggerName + "`;");
                    }
                } else {
                    log.info("(Drop statements not included. Use --drop to include them.)");
                }
                System.out.println();
            }

            if (onlyInSource.isEmpty() && onlyInTarget.isEmpty()) {
                log.info("=== No trigger differences found between source and target schemas ===");
            }
        } else {
            // JSON format - return array of SQL statements directly
            // Handle triggers only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                for (String triggerName : onlyInSource) {
                    String createStatement = getCreateTriggerStatement(sourceSqlUtils, sourceSchema, triggerName);
                    allSqlStatements.add(createStatement + ";");
                }
            }

            // Handle triggers only in target - generate DROP statements
            if (!onlyInTarget.isEmpty()) {
                if (includeDrop) {
                    for (String triggerName : onlyInTarget) {
                        allSqlStatements.add("DROP TRIGGER `" + triggerName + "`;");
                    }
                }
            }

            // Output JSON as array of SQL statements
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(allSqlStatements));
        }
    }

    private void compareEvents(SqlUtils sourceSqlUtils, SqlUtils targetSqlUtils, String sourceSchema, String targetSchema) throws Exception {
        // Get events from source
        List<Map<String, String>> sourceEvents = getEvents(sourceSqlUtils, sourceSchema);
        Set<String> sourceEventNames = sourceEvents.stream()
                .map(event -> event.get("EVENT_NAME"))
                .collect(Collectors.toSet());

        // Get events from target
        List<Map<String, String>> targetEvents = getEvents(targetSqlUtils, targetSchema);
        Set<String> targetEventNames = targetEvents.stream()
                .map(event -> event.get("EVENT_NAME"))
                .collect(Collectors.toSet());

        // Find differences
        Set<String> onlyInSource = new HashSet<>(sourceEventNames);
        onlyInSource.removeAll(targetEventNames);

        Set<String> onlyInTarget = new HashSet<>(targetEventNames);
        onlyInTarget.removeAll(sourceEventNames);

        // For JSON format, collect all SQL statements in a single list
        List<String> allSqlStatements = new ArrayList<>();

        // Output results in text format
        if ("text".equals(format)) {
            // Handle events only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                log.info("=== Events only in source ({}) ===", sourceSchema);
                for (String eventName : onlyInSource) {
                    String createStatement = getCreateEventStatement(sourceSqlUtils, sourceSchema, eventName);
                    System.out.println(createStatement + ";");
                    System.out.println();
                }
            }

            // Handle events only in target - generate DROP statements
            if (!onlyInTarget.isEmpty()) {
                log.info("=== Events only in target ({}) ===", targetSchema);
                if (includeDrop) {
                    for (String eventName : onlyInTarget) {
                        System.out.println("DROP EVENT `" + eventName + "`;");
                    }
                } else {
                    log.info("(Drop statements not included. Use --drop to include them.)");
                }
                System.out.println();
            }

            if (onlyInSource.isEmpty() && onlyInTarget.isEmpty()) {
                log.info("=== No event differences found between source and target schemas ===");
            }
        } else {
            // JSON format - return array of SQL statements directly
            // Handle events only in source - generate CREATE statements
            if (!onlyInSource.isEmpty()) {
                for (String eventName : onlyInSource) {
                    String createStatement = getCreateEventStatement(sourceSqlUtils, sourceSchema, eventName);
                    allSqlStatements.add(createStatement + ";");
                }
            }

            // Handle events only in target - generate DROP statements
            if (!onlyInTarget.isEmpty()) {
                if (includeDrop) {
                    for (String eventName : onlyInTarget) {
                        allSqlStatements.add("DROP EVENT `" + eventName + "`;");
                    }
                }
            }

            // Output JSON as array of SQL statements
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(allSqlStatements));
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

    private String getCreateTableStatement(SqlUtils sqlUtils, String schema, String tableName) throws Exception {
        String sql = "SHOW CREATE TABLE `" + schema + "`.`" + tableName + "`";
        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        if (result != null && !result.isEmpty()) {
            ONode o = ONode.loadStr(result.get(0));
            return o.get("Create Table").getString();
        }
        return "-- Could not retrieve CREATE statement for " + tableName;
    }

    private String getCreateViewStatement(SqlUtils sqlUtils, String schema, String viewName) throws Exception {
        String sql = "SHOW CREATE VIEW `" + schema + "`.`" + viewName + "`";
        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        if (result != null && !result.isEmpty()) {
            ONode o = ONode.loadStr(result.get(0));
            return o.get("Create View").getString();
        }
        return "-- Could not retrieve CREATE statement for view " + viewName;
    }

    private String getCreateRoutineStatement(SqlUtils sqlUtils, String schema, String routineName, String routineType) throws Exception {
        String sql = "SHOW CREATE " + routineType.toUpperCase() + " `" + schema + "`.`" + routineName + "`";
        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        if (result != null && !result.isEmpty()) {
            ONode o = ONode.loadStr(result.get(0));
            return o.get("Create " + routineType.substring(0, 1).toUpperCase() + routineType.substring(1)).getString();
        }
        return "-- Could not retrieve CREATE statement for " + routineType + " " + routineName;
    }

    private String getCreateTriggerStatement(SqlUtils sqlUtils, String schema, String triggerName) throws Exception {
        String sql = "SHOW CREATE TRIGGER `" + schema + "`.`" + triggerName + "`";
        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        if (result != null && !result.isEmpty()) {
            ONode o = ONode.loadStr(result.get(0));
            return o.get("SQL Original Statement").getString();
        }
        return "-- Could not retrieve CREATE statement for trigger " + triggerName;
    }

    private String getCreateEventStatement(SqlUtils sqlUtils, String schema, String eventName) throws Exception {
        String sql = "SHOW CREATE EVENT `" + schema + "`.`" + eventName + "`";
        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        if (result != null && !result.isEmpty()) {
            ONode o = ONode.loadStr(result.get(0));
            return o.get("Create Event").getString();
        }
        return "-- Could not retrieve CREATE statement for event " + eventName;
    }

    private String getColumnDefinition(Map<String, String> column) {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(column.get("COLUMN_NAME")).append("` ");

        // Use COLUMN_TYPE which includes length, precision, scale, etc.
        String columnType = column.get("COLUMN_TYPE");
        if (columnType != null && !columnType.isEmpty()) {
            sb.append(columnType);
        } else {
            // Fallback to DATA_TYPE if COLUMN_TYPE is not available
            sb.append(column.get("DATA_TYPE"));
        }

        // Add nullability
        if ("NO".equals(column.get("IS_NULLABLE"))) {
            sb.append(" NOT NULL");
        } else {
            sb.append(" NULL");
        }

        // Add default value
        String defaultValue = column.get("COLUMN_DEFAULT");
        if (defaultValue != null) {
            if ("NULL".equals(defaultValue)) {
                sb.append(" DEFAULT NULL");
            } else {
                // Handle different types of default values
                if (defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                    // Already quoted
                    sb.append(" DEFAULT ").append(defaultValue);
                } else if (defaultValue.matches("-?\\d+(\\.\\d+)?")) {
                    // Numeric value
                    sb.append(" DEFAULT ").append(defaultValue);
                } else if (defaultValue.equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                    // Function call
                    sb.append(" DEFAULT ").append(defaultValue);
                } else {
                    // String value, needs quoting
                    sb.append(" DEFAULT '").append(defaultValue).append("'");
                }
            }
        }

        // Add extra information (AUTO_INCREMENT, etc.)
        String extra = column.get("EXTRA");
        if (extra != null && !extra.isEmpty()) {
            sb.append(" ").append(extra.toUpperCase());
        }

        // Add column comment
        String comment = column.get("COLUMN_COMMENT");
        if (comment != null && !comment.isEmpty()) {
            sb.append(" COMMENT '").append(comment.replace("'", "''")).append("'");
        }

        return sb.toString();
    }

    private List<Map<String, String>> getColumns(SqlUtils sqlUtils, String schema, String table) throws Exception {
        String sql = "SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, " +
                     "CHARACTER_SET_NAME, COLLATION_NAME, EXTRA, COLUMN_KEY, COLUMN_COMMENT, ORDINAL_POSITION " +
                     "FROM information_schema.COLUMNS" +
                     " WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = '" + table + "'" +
                     " ORDER BY ORDINAL_POSITION";

        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        return parseResult(result);
    }

    private List<Map<String, String>> getViews(SqlUtils sqlUtils, String schema) throws Exception {
        String sql = "SELECT TABLE_NAME, TABLE_SCHEMA FROM information_schema.VIEWS";
        if (schema != null && !schema.isEmpty()) {
            sql += " WHERE TABLE_SCHEMA = '" + schema + "'";
        } else {
            sql += " WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')";
        }

        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        return parseResult(result);
    }

    private List<Map<String, String>> getRoutines(SqlUtils sqlUtils, String schema, String routineType) throws Exception {
        String sql = "SELECT ROUTINE_NAME FROM information_schema.ROUTINES" +
                     " WHERE ROUTINE_TYPE = '" + routineType.toUpperCase() + "'";
        if (schema != null && !schema.isEmpty()) {
            sql += " AND ROUTINE_SCHEMA = '" + schema + "'";
        } else {
            sql += " AND ROUTINE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')";
        }

        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        return parseResult(result);
    }

    private List<Map<String, String>> getTriggers(SqlUtils sqlUtils, String schema) throws Exception {
        String sql = "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS";
        if (schema != null && !schema.isEmpty()) {
            sql += " WHERE TRIGGER_SCHEMA = '" + schema + "'";
        } else {
            sql += " WHERE TRIGGER_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')";
        }

        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        return parseResult(result);
    }

    private List<Map<String, String>> getEvents(SqlUtils sqlUtils, String schema) throws Exception {
        String sql = "SELECT EVENT_NAME FROM information_schema.EVENTS";
        if (schema != null && !schema.isEmpty()) {
            sql += " WHERE EVENT_SCHEMA = '" + schema + "'";
        } else {
            sql += " WHERE EVENT_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')";
        }

        List<String> result = sqlUtils.sql(sql).queryRowList(String.class);
        return parseResult(result);
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

    // Removed outputResult method as we now output JSON directly as an array of SQL statements

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