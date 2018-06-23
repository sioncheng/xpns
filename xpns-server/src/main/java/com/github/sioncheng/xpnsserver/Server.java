package com.github.sioncheng.xpnsserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class Server {

    public boolean start(int port, int nettyEventLoopGroupThreads) throws Exception {
        eventLoopGroup = new NioEventLoopGroup(nettyEventLoopGroupThreads);
        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup);
        serverBootstrap.option(ChannelOption.SO_BACKLOG, 256);
        serverBootstrap.channel(NioServerSocketChannel.class);
        serverBootstrap.childHandler(new ClientChannelInitializer());

        serverBootstrap.bind(port).sync();

        return true;
    }

    public void stop() {
        eventLoopGroup.shutdownGracefully();
    }

    private NioEventLoopGroup eventLoopGroup;
    private ServerBootstrap serverBootstrap;
}
