package com.dyrnq.dbops.command;


import cn.hutool.json.JSONUtil;
import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "db", aliases = {"d"}, description = "database list")
public class Db extends CommonOptions implements Callable<Integer> {


    @Override
    public Integer call() throws Exception {
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        String sql = "SELECT * FROM information_schema.SCHEMATA";
        List<Map> mapSchema = sqlUtils.sql(sql).queryRowList(Map.class);
        System.out.println(JSONUtil.toJsonPrettyStr(mapSchema));
        return 0;
    }
}

