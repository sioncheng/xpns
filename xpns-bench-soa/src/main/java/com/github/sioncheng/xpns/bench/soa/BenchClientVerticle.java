package com.github.sioncheng.xpns.bench.soa;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;

public class BenchClientVerticle extends AbstractVerticle {

    public BenchClientVerticle(String targetHost) {
        this.targetHost = targetHost;
        this.url = String.format("http://%s:8000/notification",this.targetHost);
        this.counter = 0;
    }

    @Override
    public void start() throws Exception {
        httpClient = vertx.createHttpClient();
        vertx.eventBus().consumer(Events.MESSAGE_ADDRESS, this::messageHandler);
        super.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

    private void messageHandler(Message<String> message) {
        try {
            byte[] postData = message.body().getBytes("UTF-8");
            httpClient.postAbs(url, res -> {
                this.counter += 1;
                if (this.counter % 100 == 0) {
                    System.out.println(String.format("sent messages %d at %s", this.counter, this.deploymentID()));
                }
            }).exceptionHandler(t-> {
                t.printStackTrace();
            })
            .putHeader("Content-Type", "application/json;charset=UTF-8")
            .putHeader("Content-Length", String.valueOf(postData.length))
            .write(Buffer.buffer(postData))
            .end();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private HttpClient httpClient;
    private String targetHost;
    private String url;

    private int counter;
}
