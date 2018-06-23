package com.github.sioncheng.xpnsserver;


import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel nioSocketChannel) {
        nioSocketChannel.pipeline().addLast(new CommandDecoder());
        nioSocketChannel.pipeline().addLast(new CommandEncoder());
        nioSocketChannel.pipeline().addLast(new ClientChannel());
    }
}

