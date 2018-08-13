package com.github.sioncheng.xpns.bench.soa;

import io.vertx.core.AbstractVerticle;

import java.util.Random;

public class TasksVerticle extends AbstractVerticle  {

    public TasksVerticle(int messages, int seconds, String prefixChars, int maxClients) {
        this.messages = messages;
        this.seconds = seconds;
        this.prefixChars = prefixChars.split(",");
        this.maxClients = maxClients;
        this.timerCounter = 0;
        this.messagesCounter = 0;
        this.random = new Random();
    }

    @Override
    public void start() throws Exception {
        super.start();

        vertx.setTimer(1000, this::timerHandler);
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

    private void timerHandler(long l) {
        for (int i = 0 ; i < this.messages; i++) {
            int mi = this.messagesCounter;
            int h = mi % this.prefixChars.length;
            String prefix = this.prefixChars[h];
            this.messagesCounter += 1;
            sendMessage(generateClientId(prefix), mi);
        }

        System.out.println(String.format("generated messages %d at round %d", this.messages, this.timerCounter));

        this.timerCounter += 1;
        if (this.timerCounter < this.seconds) {
            vertx.setTimer(1000, this::timerHandler);
        }
    }

    private void sendMessage(String clientId, int mi) {
        String message = "{\"acid\":\"%s\", \"to\":\"%s\"," +
                "\"title\":\"title\", " +
                "\"body\":\"body %d\", \"ext\":{}, \"ttl\":3600}";

        vertx.eventBus().send(Events.MESSAGE_ADDRESS,
                String.format(message, clientId, clientId, mi));
    }

    private String generateClientId(String prefix) {
        StringBuilder sb1 = new StringBuilder(10);
        for (int i = 0 ; i < 10; i++) {
            sb1.append(prefix);
        }
        String sn = String.valueOf(random.nextInt(this.maxClients));

        int zeros = 10 - sn.length();
        StringBuilder sb2 = new StringBuilder(zeros);
        for (int i = 0 ; i < zeros; i++) {
            sb2.append("0");
        }

        return "1_" + sb1.toString() + sb2.toString() + sn;
    }

    private int messages;
    private int seconds;
    private String[] prefixChars;
    private int maxClients;

    private int timerCounter;
    private int messagesCounter;

    private Random random;

}
