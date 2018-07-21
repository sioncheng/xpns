package com.github.sioncheng.xpns.server.akka;

import akka.actor.*;
import com.github.sioncheng.xpns.common.config.AppProperties;

import java.io.IOException;

public class MainApp {

    public static void main(String[] args) throws IOException {

        AppProperties.init();

        ActorSystem actorSystem = ActorSystem.create("xpns-server-akka-tcp");

        ActorRef server = actorSystem.actorOf(ServerActor.props());

        server.tell(new ServerActor.Start(), null);

        ActorSystem actorSystem2 = ActorSystem.create("xpns-server-akka-api");

        ActorRef apiServer = actorSystem2.actorOf(ApiActor.props(AppProperties.getString("server.api.host"),
                AppProperties.getInt("server.api.port"),
                server));


        System.in.read();

        actorSystem.terminate();
        actorSystem2.terminate();
    }
}
