package com.github.sioncheng.xpnsclient;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class MainApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("vertx.logger-delegate-factory-class-name",
                "io.vertx.core.logging.SLF4JLogDelegateFactory");

        CommandArguments commandArguments = CommandArguments.readFromSystemIn();

        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setEventLoopPoolSize(commandArguments.getThreads());

        Vertx vertx = Vertx.vertx(vertxOptions);

        int batch = commandArguments.getClientsNumber() / commandArguments.getThreads();
        int startId = 1;
        for (int i = 0 ; i < commandArguments.getThreads() - 1; i++) {

            vertx.deployVerticle(new XpnsClientsVerticle(commandArguments.getAppId(),
                    startId,
                    startId + batch,
                    commandArguments.getPrefixChar(),
                    batch,
                    commandArguments.getTargetHost(),
                    commandArguments.getTargetPort()));

            startId = startId + batch;
        }

        vertx.deployVerticle(new XpnsClientsVerticle(commandArguments.getAppId(),
                startId,
                commandArguments.getClientsNumber(),
                commandArguments.getPrefixChar(),
                commandArguments.getClientsNumber() - startId + 1,
                commandArguments.getTargetHost(),
                commandArguments.getTargetPort()));


        System.in.read();
    }
}
