package com.github.sioncheng.xpnsserver;


import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    public ClientChannelInitializer(ClientChannelEventListener clientEventListener) {
        this.eventListener = clientEventListener;
    }

    @Override
    protected void initChannel(SocketChannel nioSocketChannel) {
        nioSocketChannel.pipeline().addLast(new CommandDecoder());
        nioSocketChannel.pipeline().addLast(new CommandEncoder());
        nioSocketChannel.pipeline().addLast(new ClientChannel(this.eventListener));
    }

    private ClientChannelEventListener eventListener;
}

