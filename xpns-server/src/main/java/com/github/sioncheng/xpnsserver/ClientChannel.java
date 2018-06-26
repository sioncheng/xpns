package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.protocol.Command;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientChannel extends SimpleChannelInboundHandler<Command> {

    public ClientChannel(ClientChannelEventListener clientChannelEventListener) {
        this.clientChannelEventListener = clientChannelEventListener;
    }

    public void writeCommand(Command command) {
        if (this.ctx != null) {
            this.ctx.channel().writeAndFlush(command);
        }
    }

    public void shutdown() {
       if (this.ctx == null) {
           return;
       }

       this.ctx.channel().disconnect();
       this.ctx.channel().close();
       this.ctx.disconnect();
       this.ctx.close();
       this.ctx = null;
    }

    public String getAcid() {
        return acid;
    }

    public void setAcid(String acid) {
        this.acid = acid;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        this.ctx = ctx;

        if (this.clientChannelEventListener != null) {
            this.clientChannelEventListener.clientChannelActive(this);
        }

        if (logger.isInfoEnabled()) {
            logger.info("channel active {}", ctx.channel().remoteAddress().toString());
        }

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        if (this.clientChannelEventListener != null) {
            this.clientChannelEventListener.clientChannelInactive(this);
        }

        if (logger.isInfoEnabled()) {
            logger.info("channel inactive {}", ctx.channel().remoteAddress().toString());
        }

        super.channelInactive(ctx);
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

    private static final Logger logger = LoggerFactory.getLogger(ClientChannel.class);
}
