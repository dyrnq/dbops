package com.dyrnq.dbops.command;

import picocli.CommandLine;

public class CommonOptions {
    @CommandLine.Option(names = {"-v", "--verbose"}, description = "explain what is being done")
    boolean verbose;
    @CommandLine.Option(names = {"-s", "--sql"}, description = "sql", defaultValue = "select 1=1;")
    String sql;
    @CommandLine.Option(names = {"--ds", "-ds"}, description = "datasource name", defaultValue = "default")
    String ds;
}
