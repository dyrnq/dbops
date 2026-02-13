package com.dyrnq.dbops.command;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import java.io.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "wood", aliases = {"gen"}, description = "Wood gen <https://gitee.com/noear/wood>")
@Slf4j
public class WoodGen extends CommonOptions implements Callable<Integer> {

    private static final Pattern LOWER_CASE = Pattern.compile("[a-z]");
    private static final String DEFAULT_DOMAIN_SUBPACKAGE_NAME = "model";
    private static final String DEFAULT_MAPPER_SUBPACKAGE_NAME = "dso";
    @CommandLine.Option(names = {"--package-name", "-p"}, description = "package name", defaultValue = "com.example")
    String package_name;
    @CommandLine.Option(names = {"--domain-package-name", "--domain-pkg-name", "-dp"}, description = "domain package name")
    String domain_package_name;
    @CommandLine.Option(names = {"--mapper-package-name", "--mapper-pkg-name", "-mp"}, description = "mapper package name")
    String mapper_package_name;

    @CommandLine.Option(names = {"--template-dir", "-T"}, description = "templateDir", defaultValue = "src/main/resources/templates")
    String templateDir;

    @CommandLine.Option(names = {"--dist-dir", "-D"}, description = "distDir", defaultValue = "src/main/java/")
    String distDir;

    @CommandLine.Option(names = {"--openapi", "--open", "-open"}, description = "openapi", defaultValue = "false")
    boolean openapi;

    @CommandLine.Option(names = {"--customize-begin", "--begin", "-b"}, description = "distDir", defaultValue = "//Customize BEGIN")
    String customize_begin;

    @CommandLine.Option(names = {"--customize-end", "--end", "-e"}, description = "distDir", defaultValue = "//Customize END")
    String customize_end;


    public static String toCamelString(String name) {
        return toCamelString(name, false);
    }

    /**
     * 如果全是大写字母, 则转换为小写字母
     *
     * @param name 待转换的名称
     * @return 转换后的名称
     */
    public static String toLowerCaseIfAllUpperCase(String name) {
        if (name == null || name.length() == 0) {
            return name;
        }
        if (LOWER_CASE.matcher(name).find()) {
            return name; // 有小写字母, 不处理, 直接返回
        } else {
            return name.toLowerCase(); // 整个字符串都没有小写字母则转换为小写字母
        }
    }

    public static String toCamelString(String name, boolean startsWithUpperCase) {
        if (name == null || name.length() == 0) {
            return name;
        }
        char[] chars = toLowerCaseIfAllUpperCase(name.trim()).toCharArray();

        StringBuilder buffer = new StringBuilder();
        boolean underline = startsWithUpperCase;
        for (char c : chars) {
            if (c == '_') {
                underline = true;
            } else {
                if (underline) {
                    buffer.append(Character.toUpperCase(c));
                } else {
                    buffer.append(c);
                }
                underline = false;
            }
        }
        return buffer.toString();
    }

    public static Set<String> getPrimaryKeys(Connection conn, String schema, String tableName) throws SQLException {
        Set<String> primaryKeys = new HashSet<>();
        DatabaseMetaData metaData = conn.getMetaData();

        try (ResultSet pkRs = metaData.getPrimaryKeys(null, schema, tableName)) {
            while (pkRs.next()) {
                String columnName = pkRs.getString("COLUMN_NAME");
                primaryKeys.add(columnName);
            }
        }
        return primaryKeys;
    }

    private String dbTypeToJavaType(String dbType) {

        String up = dbType.toUpperCase();

        if (StringUtils.startsWith(up, "INT")) {
            return "Integer";
        } else if (StringUtils.startsWith(up, "TINYINT")) {
            return "Integer";
        } else if (StringUtils.startsWith(up, "SMALLINT")) {
            return "Integer";
        } else if (StringUtils.startsWith(up, "BIGINT")) {
            return "Long";
        } else if (StringUtils.startsWith(up, "TIMESTAMP")) {
            return "java.time.LocalDateTime";
        } else if (StringUtils.equalsIgnoreCase(up, "DATE")) {
            return "java.time.LocalDateTime";
        } else if (StringUtils.equalsIgnoreCase(up, "DATETIME")) {
            return "java.time.LocalDateTime";
        }


        return "String";


    }


    public Map<String, String> getTableCommentMap(String database) throws SQLException {
        String sql = "SELECT table_name,table_comment FROM information_schema.TABLES WHERE table_schema = ?";
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        List<Map> infoSchemas = sqlUtils.sql(sql, database).queryRowList(Map.class);
        Map<String, String> map = new HashMap<>();

        infoSchemas.forEach(infoSchema -> {
            map.put(ObjUtil.toString(infoSchema.get("TABLE_NAME")), ObjUtil.toString(infoSchema.get("TABLE_COMMENT")));
        });
        return map;
    }


    public String getCurrentDatabase() throws SQLException {
        String sql = "SELECT DATABASE()";
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        return sqlUtils.sql(sql).queryValue();
    }


    @Override
    public Integer call() throws Exception {

        String distPath = StrUtil.endWith(distDir, File.separator) ? distDir.substring(0, distDir.length() - 1) : distDir;

        Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
        cfg.setDirectoryForTemplateLoading(new File(templateDir));
        Template modelTpl = cfg.getTemplate("ModelClass.tpl");
        Template mapperTpl = cfg.getTemplate("MapperClass.tpl");


        String domain_pkg = StrUtil.isBlank(domain_package_name) ? (package_name + "." + DEFAULT_DOMAIN_SUBPACKAGE_NAME) : domain_package_name;
        String mapper_pkg = StrUtil.isBlank(mapper_package_name) ? (package_name + "." + DEFAULT_MAPPER_SUBPACKAGE_NAME) : mapper_package_name;


        Map<String, String> skipTables = new LinkedHashMap<>();


        // 获取所有表名
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        List<String> tableNames = sqlUtils.sql("SHOW TABLES").queryValueList();


        String schema = getCurrentDatabase();

        Map<String, String> tableCommentMap = getTableCommentMap(schema);

        for (String tableName : tableNames) {
            if (skipTables.containsKey(tableName)) {
                continue;
            }
            Map<String, Object> data = new HashMap<>();
            // 获取表的元数据
            DatabaseMetaData metaData = sqlUtils.getDataSource().getConnection().getMetaData();

            Set<String> primaryKeys = new HashSet<>();

            try (ResultSet pkRs = metaData.getPrimaryKeys(null, schema, tableName)) {
                while (pkRs.next()) {
                    String columnName = pkRs.getString("COLUMN_NAME");
                    primaryKeys.add(columnName);
                }
            }
            if (primaryKeys.isEmpty()) {
                primaryKeys.add("id");
            }
            ResultSet columns = metaData.getColumns(schema, schema, tableName, "%");
            String domainName = toCamelString(tableName, true);
            String mapperName = domainName + "Mapper";
            data.put("table", tableName);
            data.put("domain", domainName);
            data.put("mapper", mapperName);
            data.put("schema", schema);
            data.put("full_domain", domain_pkg + "." + domainName);
            data.put("package_name", domain_pkg);
            data.put("mapper_pkg", mapper_pkg);
            data.put("mapper_package_name", mapper_pkg);
            data.put("customize_begin", customize_begin);
            data.put("customize_end", customize_end);
            data.put("customize_content", "");
            data.put("openapi", openapi);
            String tableComment = "";
            try {
                tableComment = tableCommentMap.get(tableName);
            } catch (Exception exception) {
                exception.printStackTrace();
            }

            if (StrUtil.isBlank(tableComment)) {
                tableComment = tableName;
            }

            data.put("database", schema);
            data.put("tableComment", tableComment);
            List<Map<String, Object>> fieldList = new ArrayList<>();
            while (columns.next()) {
                Map<String, Object> item = new LinkedHashMap<>();
                String columnName = columns.getString("COLUMN_NAME");
                String columnType = columns.getString("TYPE_NAME");
                int columnLength = columns.getInt("COLUMN_SIZE");
                String columnComment = columns.getString("REMARKS");

                item.put("pk", primaryKeys.contains(columnName));
                item.put("columnName", columnName);
                item.put("fieldName", toCamelString(columnName));
                item.put("fieldComment", columnComment);
                item.put("columnComment", columnComment);
                item.put("fieldType", dbTypeToJavaType(columnType));
                item.put("columnType", columnType);
                item.put("columnLength", columnLength);
                item.put("finalFieldName", StringUtils.upperCase(columnName));
                fieldList.add(item);
            }
            data.put("fieldList", fieldList);
            String outputPath = StringUtils.joinWith(File.separator, distPath
                    , StringUtils.replace(domain_pkg, ".", File.separator), domainName
            ) + ".java";
            FileUtils.forceMkdirParent(new File(outputPath));
            if (FileUtil.isExistsAndNotDirectory(new File(outputPath).toPath(), false)) {
                String content = IoUtil.readUtf8(new FileInputStream(new File(outputPath)));
                String oldC = StringUtils.substringBetween(content, customize_begin, customize_end);
                oldC = StringUtils.trim(oldC);
                data.put("customize_content", oldC);
            } else {
                data.put("customize_content", "");
            }


            try (Writer out = new OutputStreamWriter(new FileOutputStream(outputPath))) {
                modelTpl.process(data, out);
            }


            String mapper_outputPath = StringUtils.joinWith(File.separator, distPath
                    , StringUtils.replace(mapper_pkg, ".", File.separator), mapperName
            ) + ".java";


            FileUtils.forceMkdirParent(new File(mapper_outputPath));
            if (FileUtil.isExistsAndNotDirectory(new File(mapper_outputPath).toPath(), false)) {
                String content = IoUtil.readUtf8(new FileInputStream(new File(mapper_outputPath)));
                String oldC = StringUtils.substringBetween(content, customize_begin, customize_end);
                oldC = StringUtils.trim(oldC);
                data.put("customize_content", oldC);
            } else {
                data.put("customize_content", "");
            }

            try (Writer out = new OutputStreamWriter(new FileOutputStream(mapper_outputPath))) {
                mapperTpl.process(data, out);
            }


        }


        return 0;
    }

}
