package com.dyrnq.dbops.command;

import cn.hutool.json.JSONUtil;
import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "process", aliases = {"ps"}, description = "process list")
public class Process extends CommonOptions implements Callable<Integer> {
    @CommandLine.Option(names = {"--full"})
    boolean full;

    @Override
    public Integer call() throws Exception {
        String sql = "SHOW PROCESSLIST;";
        if (full) {
            sql = "SHOW FULL PROCESSLIST;";
        }
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        System.out.println(JSONUtil.toJsonPrettyStr(sqlUtils.sql(sql).queryRowList(Map.class)));
        return 0;
    }
}
