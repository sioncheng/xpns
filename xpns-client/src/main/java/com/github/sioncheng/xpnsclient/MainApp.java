package com.github.sioncheng.xpnsclient;

import com.github.sioncheng.xpns.common.console.CommandLineArgsReader;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.apache.commons.lang3.StringUtils;

public class MainApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("vertx.logger-delegate-factory-class-name",
                "io.vertx.core.logging.SLF4JLogDelegateFactory");

        CommandArguments commandArguments = null;

        String config = System.getProperty("config");

        if (StringUtils.isEmpty(config)) {
            commandArguments = CommandArguments.readFromSystemIn();
        } else {
            commandArguments = CommandArguments.readFromConfig(config);
        }
        CommandLineArgsReader.refillFromSystemD(commandArguments);


        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setEventLoopPoolSize(commandArguments.getThreads());

        Vertx vertx = Vertx.vertx(vertxOptions);

        int batch = commandArguments.getClientsNumber() / commandArguments.getThreads();

        int startId = 1;
        for (int i = 0 ; i < commandArguments.getThreads(); i++) {
            int sid = startId;
            int tid = startId + batch;
            if (i == commandArguments.getThreads() - 1) {
                sid = startId;
                tid = commandArguments.getClientsNumber();
            }

            vertx.deployVerticle(new XpnsClientsVerticle(commandArguments.getAppId(),
                    sid,
                    tid,
                    commandArguments.getPrefixChar(),
                    tid - sid + 1,
                    commandArguments.getTargetHost(),
                    commandArguments.getTargetPort()));

            startId = startId + batch;
        }

        System.in.read();
    }
}
