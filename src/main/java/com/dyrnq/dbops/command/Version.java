package com.dyrnq.dbops.command;

import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;


@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "version", aliases = {"ver"}, description = "Version")
public class Version extends CommonOptions implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        sql = "select version();";
        System.out.print(sqlUtils.sql(sql).queryRow(String.class));
        return 0;
    }
}