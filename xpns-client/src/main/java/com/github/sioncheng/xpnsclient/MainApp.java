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


        vertx.deployVerticle(new XpnsClientsVerticle(commandArguments.getAppId(),
                1,
                commandArguments.getClientsNumber() + 1,
                commandArguments.getPrefixChar(),
                commandArguments.getClientsNumber(),
                commandArguments.getTargetHost(),
                commandArguments.getTargetPort()));

        System.in.read();
    }
}
