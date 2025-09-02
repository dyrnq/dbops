package com.dyrnq.dbops.command;

import com.dyrnq.dbops.utils.BCryptPasswordEncoder;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        name = "bcrypt", aliases = {"bc"}, description = "BCryptPassword")
public class BCryptPassword implements Callable<Integer> {

    @CommandLine.Option(names = {"-p", "--pass"}, description = "pass")
    String pass;
    @CommandLine.Option(names = {"-s", "--strength"}, description = "strength", defaultValue = "12")
    int strength;

    @Override
    public Integer call() throws Exception {
        BCryptPasswordEncoder bCryptPasswordEncoder = new BCryptPasswordEncoder(strength);
        System.out.print(bCryptPasswordEncoder.encode(pass));
        return 0;
    }
}
