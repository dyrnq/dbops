package com.dyrnq.dbops.command;

import org.noear.solon.data.sql.SqlUtils;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "exec", aliases = {"e"}, description = "Exec")
public class Exec extends CommonOptions implements Callable<Integer> {

    @CommandLine.Option(names = {"-A", "--always-list"})
    boolean alwaysList;

    @Override
    public Integer call() throws Exception {
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        List<String> list = sqlUtils.sql(sql).queryRowList(String.class);
        if (alwaysList) {
            System.out.println(list);
        } else {
            if (list != null) {
                if (list.isEmpty()) {
                    System.out.print("{}");
                } else if (list.size() == 1) {
                    System.out.print(list.get(0));
                } else {
                    System.out.print(list);
                }

            } else {
                System.out.print("{}");
            }
        }

        return 0;
    }
}
