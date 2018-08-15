package com.github.sioncheng.xpns.bench.soa;

import io.vertx.core.Vertx;

public class MainApp {

    public static void main(String[] args) throws Exception {
        System.out.println("xpns bench soa");

        CommandArguments commandArguments = CommandArguments.readFromSystemIn();

        Vertx vertx = Vertx.vertx();

        int processors = Runtime.getRuntime().availableProcessors();

        for (int i = 0 ; i < processors; i++) {
            vertx.deployVerticle(new BenchClientVerticle(commandArguments.getTargetHost()));
        }

        vertx.deployVerticle(new TasksVerticle(commandArguments.getMessages(),
                commandArguments.getSeconds(),
                commandArguments.getPrefixChars(),
                commandArguments.getMaxClients()));

        System.in.read();

        vertx.close();
    }
}
