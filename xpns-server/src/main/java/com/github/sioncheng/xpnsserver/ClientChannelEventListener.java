package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.protocol.Command;

interface ClientChannelEventListener {

    void clientChannelActive(ClientChannel clientChannel);

    void commandIn(Command command, ClientChannel clientChannel);

    void clientChannelInactive(ClientChannel clientChannel);
}
