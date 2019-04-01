package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.protocol.Command;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientChannel extends SimpleChannelInboundHandler<Command> {

    public ClientChannel(ClientChannelEventListener clientChannelEventListener) {
        this.clientChannelEventListener = clientChannelEventListener;
        this.publishInactive = true;
    }

    public void writeCommand(Command command) {
        if (this.ctx != null) {
            this.ctx.channel().writeAndFlush(command);
        }
    }

    public void shutdown() {
       shutdown(true);
    }

    public void shutdown(boolean publishInactive) {
        if (this.ctx == null) {
            return;
        }

        if (!publishInactive) {
            this.clientChannelEventListener = null;
        }
        this.publishInactive = publishInactive;

        this.ctx.channel().disconnect();
        this.ctx.channel().close();
        this.ctx.disconnect();
        this.ctx.close();
        this.ctx = null;

        if (logger.isInfoEnabled()) {
            logger.info("client channel shutdown {}", this.getAcid());
        }
    }

    public String getAcid() {
        return acid;
    }

    public void setAcid(String acid) {
        this.acid = acid;
    }

    public void setLogonTime(long logonTime) {
        this.logonTime = logonTime;
    }

    public long getLogonTime() {
        return this.logonTime;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        this.ctx = ctx;

        if (this.clientChannelEventListener != null) {
            this.clientChannelEventListener.clientChannelActive(this);
        }

        if (logger.isInfoEnabled()) {
            logger.info("channel active {}", ctx.channel().remoteAddress().toString());
        }


    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        if (this.clientChannelEventListener != null && this.publishInactive) {
            this.clientChannelEventListener.clientChannelInactive(this);
        }

        if (logger.isInfoEnabled()) {
            logger.info("channel inactive {}", ctx.channel().remoteAddress().toString());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        logger.warn(String.format("%s exception caught %s", this.acid, cause.getMessage()));

        this.shutdown();

        //super.exceptionCaught(ctx, cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("client %s idle state event %s",
                        acid,
                        ((IdleStateEvent) evt).state().toString()));
            }
            this.shutdown();
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Command command) {

        if (logger.isDebugEnabled()) {
            logger.debug("read command {} {} {} {} {}"
                    , command.getSerializationType()
                    , command.getCommandType()
                    , command.getSerializationType()
                    , command.getPayloadLength()
                    , command.getPayloadBytes());
        }

        if (this.clientChannelEventListener != null) {
            this.clientChannelEventListener.commandIn(command, this);
        }

    }

    private ClientChannelEventListener clientChannelEventListener;
    private ChannelHandlerContext ctx;
    private volatile String acid;
    private volatile boolean publishInactive;
    private volatile long logonTime;

    private static final Logger logger = LoggerFactory.getLogger(ClientChannel.class);
}
