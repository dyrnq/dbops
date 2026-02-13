package com.dyrnq.dbops.command;

import com.dyrnq.dbops.Constants;
import picocli.CommandLine;

import java.util.concurrent.Callable;


@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "version", aliases = {"ver"}, description = "Version")
public class Version extends CommonOptions implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        System.out.println(Constants.VERSION);
        return 0;
    }
}