package com.github.sioncheng.xpnsserver;


import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    public ClientChannelInitializer(ClientChannelEventListener clientEventListener) {
        this.eventListener = clientEventListener;
    }

    @Override
    protected void initChannel(SocketChannel sc) {
        if (logger.isInfoEnabled()) {
            logger.info("init channel {}", sc.remoteAddress().toString());
        }
        sc.pipeline().addLast(new CommandDecoder());
        sc.pipeline().addLast(new CommandEncoder());
        sc.pipeline().addLast(new IdleStateHandler(360, 0, 0, TimeUnit.SECONDS));
        sc.pipeline().addLast(new ClientChannel(this.eventListener));
    }

    private ClientChannelEventListener eventListener;

    private static final Logger logger = LoggerFactory.getLogger(ClientChannelInitializer.class);
}

