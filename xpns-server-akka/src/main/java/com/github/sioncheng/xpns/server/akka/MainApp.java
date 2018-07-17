package com.github.sioncheng.xpns.server.akka;

import akka.actor.*;
import com.github.sioncheng.xpns.common.config.AppProperties;

import java.io.IOException;

public class MainApp {

    public static void main(String[] args) throws IOException {

        AppProperties.init();

        ActorSystem actorSystem = ActorSystem.create("xpns-server-akka");

        ActorRef server = actorSystem.actorOf(ServerActor.props());

        server.tell(new ServerActor.Start(), null);

        System.in.read();

        actorSystem.terminate();
    }
}
