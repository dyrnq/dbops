package com.dyrnq.dbops.command;

import cn.hutool.json.JSONUtil;
import com.dyrnq.dbops.Constants;
import org.noear.snack4.ONode;
import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "info", aliases = {"i"}, description = "Return low-level information")
public class Info extends CommonOptions implements Callable<Integer> {
    @CommandLine.Option(names = {"--vars"})
    boolean vars;

    @Override
    public Integer call() throws Exception {
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        Map<String, Object> objectMap = new LinkedHashMap<>();
        objectMap.put("java_version", System.getProperty("java.version"));
        objectMap.put("jvm_version", System.getProperty("java.version"));
        objectMap.put("version", sqlUtils.sql("select version();").queryValue());
        objectMap.put("client", Constants.VERSION);
        objectMap.put("dataSource", sqlUtils.getDataSource());
        objectMap.put("now", sqlUtils.sql("select now()").queryValue());
        objectMap.put("time_zone", sqlUtils.sql("SELECT @@time_zone;").queryValue());
        objectMap.put("user", sqlUtils.sql("select user()").queryValue());
        objectMap.put("database", sqlUtils.sql("select database()").queryValue());
        //objectMap.put("processlist",sqlUtils.sql("SHOW FULL PROCESSLIST;").queryRowList(Map.class));
        objectMap.put("DEFAULT_CHARACTER_SET_NAME", sqlUtils.sql("SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = DATABASE();").queryValue());
        objectMap.put("DEFAULT_COLLATION_NAME", sqlUtils.sql("SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = DATABASE();").queryValue());
        objectMap.put("databases", sqlUtils.sql("SHOW DATABASES;").queryValueList());

        if (vars) {
            List<String> listS = sqlUtils.sql("show variables").queryRowList(String.class);
            Map<String, String> mapS = new LinkedHashMap<>();
            listS.forEach(c -> {
                ONode o = ONode.ofJson(c);
                mapS.put(
                        o.get("Variable_name").getString(),
                        o.get("Value").getString()
                );

            });

            objectMap.put("vars", mapS);
        }
        System.out.println(JSONUtil.toJsonPrettyStr(objectMap));
        return 0;
    }


}
