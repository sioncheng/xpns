package com.github.sioncheng.xpns.es;

import com.github.sioncheng.xpns.common.config.AppProperties;
import io.vertx.core.Vertx;

public class MainApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("vertx.logger-delegate-factory-class-name",
                "io.vertx.core.logging.SLF4JLogDelegateFactory");

        AppProperties.init();

        Vertx vertx = Vertx.vertx();

        int instances = AppProperties.getInt("server.instances");

        for (int i = 0 ; i < instances; i++) {
            ElasticsearchVerticle elasticsearchVerticle = new ElasticsearchVerticle();

            vertx.deployVerticle(elasticsearchVerticle);
        }

        System.in.read();
    }
}
