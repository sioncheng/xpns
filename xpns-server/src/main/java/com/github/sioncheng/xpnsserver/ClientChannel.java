package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.Command;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientChannel extends SimpleChannelInboundHandler<Command> {

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

        Command command1 = handleCommand(command);
        if (command1 != null) {
            channelHandlerContext.channel().writeAndFlush(command1);
        }

    }

    private Command handleCommand(Command command) {
        command.setCommandType(Command.RESPONSE);
        return command;
    }

    private static final Logger logger = LoggerFactory.getLogger(ClientChannel.class);
}
