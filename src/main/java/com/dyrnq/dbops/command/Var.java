package com.dyrnq.dbops.command;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
        name = "variables", aliases = {"var", "vars"}, description = "Variables")
public class Var extends CommonOptions implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        SqlUtils sqlUtils = SqlUtils.ofName(ds);
        sql = "show variables";

        List<String> listS = sqlUtils.sql(sql).queryRowList(String.class);
        Map<String, String> mapS = new LinkedHashMap<>();
        listS.forEach(c -> {
            ONode o = ONode.ofJson(c);
            mapS.put(
                    o.get("Variable_name").getString(),
                    o.get("Value").getString()
            );

        });
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.print(gson.toJson(mapS));
        return 0;
    }
}