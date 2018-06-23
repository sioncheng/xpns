package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.protocol.Command;

interface ClientEventListener {

    void commandIn(Command command, ClientChannel clientChannel);
}
