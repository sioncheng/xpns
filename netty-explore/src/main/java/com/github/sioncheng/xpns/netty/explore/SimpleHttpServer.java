package com.github.sioncheng.xpns.netty.explore;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.io.InputStream;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.rtsp.RtspHeaderNames.CONTENT_LENGTH;

public class SimpleHttpServer {

    public static void main(String[] args) throws Exception {

        NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(2);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(nioEventLoopGroup);

        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                // server端发送的是httpResponse，所以要使用HttpResponseEncoder进行编码
                ch.pipeline().addLast(new HttpResponseEncoder());
                // server端接收到的是httpRequest，所以要使用HttpRequestDecoder进行解码
                ch.pipeline().addLast(new HttpRequestDecoder());
                ch.pipeline().addLast(new HttpHandler());
            }
        });

        final ChannelFuture channelFuture = bootstrap.bind(0);

        channelFuture.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                System.out.println(channelFuture.channel().localAddress().toString());
            }
        });

        channelFuture.sync();
    }


    static class HttpHandler extends SimpleChannelInboundHandler {

        private boolean isFavicon = false;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof  DefaultHttpRequest) {
                final DefaultHttpRequest defaultHttpRequest = (DefaultHttpRequest)msg;
                System.out.println(defaultHttpRequest.uri());
                isFavicon = (defaultHttpRequest.uri().endsWith("favicon.ico"));
                return;
            }
            System.out.println(String.format("channelRead0 %s %s", msg == null, msg.getClass().toString()));
            ctx.channel().write(createResponse() ).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future)  {
                    System.out.println("channel write operation complete");
                    future.channel().close();
                }
            });
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx)  {
            System.out.println("channelReadComplete");
            ctx.flush();
        }

        private FullHttpResponse createResponse() throws Exception{
            if (isFavicon) {
                return favicon();
            }

            return ok();
        }

        private FullHttpResponse ok() throws IOException {
            String res = String.format("I am OK %d\r\n", System.currentTimeMillis());
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                    OK, Unpooled.wrappedBuffer(res.getBytes("UTF-8")));
            response.headers().set(CONTENT_TYPE, "text/plain");
            response.headers().set(CONTENT_LENGTH,
                    response.content().readableBytes());

            return response;
        }

        private FullHttpResponse favicon() throws IOException {
            InputStream inputStream =  HttpHandler.class.getResourceAsStream("/th.jpeg");
            byte[] bytes = new byte[inputStream.available()];
            inputStream.read(bytes);
            inputStream.close();
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                    OK, Unpooled.wrappedBuffer(bytes));
            response.headers().set(CONTENT_TYPE, "image/x-icon");
            response.headers().set(CONTENT_LENGTH,
                    response.content().readableBytes());

            return response;
        }
    }
}
