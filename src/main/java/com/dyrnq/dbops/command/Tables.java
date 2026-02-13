package com.dyrnq.dbops.command;

import cn.hutool.json.JSONUtil;
import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "tables", aliases = {"t"}, description = "print tables info")
public class Tables extends CommonOptions implements Callable<Integer> {


    @Override
    public Integer call() throws Exception {
        String sql = "SELECT * FROM information_schema.TABLES WHERE table_schema = DATABASE();";
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        System.out.println(JSONUtil.toJsonPrettyStr(sqlUtils.sql(sql).queryRowList(Map.class)));
        return 0;
    }
}
