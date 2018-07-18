package com.github.sioncheng.xpns.server.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.io.Tcp;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;

import java.util.List;

public class ClientActor extends AbstractActor {

    public static Props props(ActorRef server) {
        return Props.create(ClientActor.class,() -> new ClientActor(server));
    }

    public ClientActor(ActorRef server) {
        this.server = server;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tcp.Received.class, received -> {
                    handleReceived(received);
                })
                .match(Tcp.ConnectionClosed.class, connectionClosed -> {
                    server.tell(ClientActivation.createLogout(getSelf(), acid), getSelf());
                    getContext().stop(self());
                })
                .build();
    }

    private void handleReceived(Tcp.Received received) {
        System.out.println(Thread.currentThread().getName());
        List<JsonCommand> commands = commandCodec.decode(received.data().asByteBuffer());
        commands.forEach(c -> {
            switch (c.getCommandCode()) {
                case JsonCommand.LOGIN_CODE:
                    this.acid = c.getAcid();
                    server.tell(ClientActivation.createLogon(getSelf(), c.getAcid()), getSelf());
                    break;
            }
        });

    }

    private ActorRef server;

    private String acid;

    private CommandCodec commandCodec = new CommandCodec();
}
