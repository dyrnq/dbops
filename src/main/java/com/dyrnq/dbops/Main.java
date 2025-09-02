package com.dyrnq.dbops;

import cn.hutool.core.io.FileUtil;
import com.dyrnq.dbops.command.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.SystemUtils;
import org.noear.solon.Solon;
import picocli.CommandLine;

import java.io.File;

@picocli.CommandLine.Command(
        subcommands = {
                Exec.class,
                Var.class,
                Version.class,
                BCryptPassword.class,
                SchemaDiff.class,
                SchemaToDoris.class
        },
        mixinStandardHelpOptions = true,
        showDefaultValues = true
)
@Slf4j
public class Main implements Runnable {
    public static String homeAbsolutePath(String home, String appName) {
        String homeAbsolutePath = "";
        String systemUserDir = SystemUtils.getUserHome().getAbsolutePath();
        if (StringUtils.isBlank(home)) {
            homeAbsolutePath = StringUtils.joinWith(File.separator, systemUserDir, appName);
        } else {
            if (Strings.CS.startsWith(home, "~")) {
                homeAbsolutePath = RegExUtils.replaceFirst(home, "~", systemUserDir);
            } else {
                homeAbsolutePath = home;
            }
        }
        return homeAbsolutePath;
    }

    public static void main(String[] args) {

        Solon.start(Main.class, args, app -> {
            String homeDir = homeAbsolutePath("", "." + app.cfg().appName());
            String[] files = new String[]{"config.yaml", "config.yml"};
            for (String file : files) {
                String yamlFile = StringUtils.joinWith(File.separator, homeDir, file);
                if (FileUtil.exist(yamlFile)) {
                    log.info("load config yaml {}", yamlFile);
                    app.cfg().loadAdd(yamlFile);
                }
            }

        });

        Main app = new Main();
        CommandLine cmd = new CommandLine(app);

        int code = cmd.execute(args);

        System.exit(code);
        Solon.stop(0);


    }


    @Override
    public void run() {

    }

}