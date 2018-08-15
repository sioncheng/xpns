package com.github.sioncheng.xpnsserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class TempTest {

    @Test
    public void TestHash() {
        String acid = "1_aaaaaaaaaa0000000003";

        System.out.println(Math.abs(acid.charAt(acid.length() - 1)) % 4);

        System.out.println(UUID.randomUUID().toString());
    }

    @Test
    public void test() throws Exception {
        NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(1);

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(nioEventLoopGroup);
        serverBootstrap.channel(NioServerSocketChannel.class);
        serverBootstrap.handler(new ChannelHandler() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
                System.out.println("handler added");
            }

            @Override
            public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
                System.out.println("handler removed");
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                System.out.println("exception caught");
            }
        });
        serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                System.out.println("init channel");
            }
        });

        serverBootstrap.bind(8080).sync();

        System.in.read();
    }

    @Test
    public void testThreadPool() {
        ForkJoinPool forkJoinPool = new ForkJoinPool(1);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                sleep(300);
                System.out.println("fork join pool 1");
                System.out.println(Thread.currentThread().getId());
            }
        };
        forkJoinPool.submit(runnable);

        runnable = new Runnable() {
            @Override
            public void run() {
                sleep(300);
                System.out.println("fork join pool 2");
                System.out.println(Thread.currentThread().getId());
            }
        };
        forkJoinPool.submit(runnable);

        runnable = new Runnable() {
            @Override
            public void run() {
                sleep(300);
                System.out.println("fork join pool 3");
                System.out.println(Thread.currentThread().getId());
            }
        };
        forkJoinPool.submit(runnable);

        runnable = new Runnable() {
            @Override
            public void run() {
                sleep(300);
                System.out.println("fork join pool 4");
                System.out.println(Thread.currentThread().getId());
            }
        };
        forkJoinPool.submit(runnable);

        runnable = new Runnable() {
            @Override
            public void run() {
                sleep(300);
                System.out.println("fork join pool 5");
                System.out.println(Thread.currentThread().getId());
            }
        };
        forkJoinPool.submit(runnable);

        RecursiveTask t1 = new RecursiveTask() {
            @Override
            protected Object compute() {
                sleep(300);
                System.out.println("fork join pool 6");
                System.out.println(Thread.currentThread().getId());
                return null;
            }
        };

        forkJoinPool.submit(t1);

        ForkJoinTask f1  = t1.fork();
        ForkJoinTask f2 = t1.fork();

        f1.join();
        f2.join();


        System.out.println(Thread.currentThread().getId());

        sleep(5000);
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception ex) {

        }
    }
}
