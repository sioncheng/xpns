package com.github.sioncheng.xpns.server.akka;

import akka.actor.*;
import akka.io.Tcp;

import akka.actor.AbstractActor;
import akka.io.TcpMessage;
import com.github.sioncheng.xpns.common.config.AppProperties;

import java.net.InetSocketAddress;
import java.util.HashMap;

public class ServerActor extends AbstractActor {

    public static Props props() {
        return Props.create(ServerActor.class,() -> new ServerActor());
    }

    public ServerActor () {
        this.clients = new HashMap<>();
    }

    @Override
    public Receive createReceive() {

        return receiveBuilder()
                .match(Start.class, msg -> start())
                .match(Tcp.Bound.class, msg -> {
                    System.out.println("bound");
                })
                .match(Tcp.Connected.class, msg -> {
                    System.out.println(Thread.currentThread().getName());
                    final ActorRef clientActor = getContext().getSystem().actorOf(ClientActor.props(getSelf()));
                    getSender().tell(TcpMessage.register(clientActor), getSelf());
                })
                .match(ClientActivation.class, cl->{
                    System.out.println(String.format("client logon %s %d", cl.acid(), cl.status()));
                })
                .build();
    }

    private void start() {

        manager = Tcp.createExtension((ExtendedActorSystem)getContext().getSystem()).manager();

        manager.tell(TcpMessage.bind(getSelf(),
                new InetSocketAddress("0.0.0.0", AppProperties.getInt("server.port")),
                100),
                getSelf());
    }

    private ActorRef manager;

    private HashMap<String, ActorRef> clients;

    public final static class Start{}
}
