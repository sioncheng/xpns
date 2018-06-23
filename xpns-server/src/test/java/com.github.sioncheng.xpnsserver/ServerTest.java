package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.Command;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ServerTest {


    @Test
    public void testServerStart() throws Exception {
        Server server = new Server();
        boolean result = server.start(8080, 2);
        Assert.assertTrue(result);
        server.stop();
    }

    @Test
    public void testHandleClient() throws Exception {
        Server server = new Server();
        boolean result = server.start(8080, 2);
        Assert.assertTrue(result);

        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(8080));

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("acid", "320000000001");
        jsonObject.put("command", 1);

        byte[] payload = jsonObject.toJSONString().getBytes("UTF-8");


        socket.getOutputStream().write(Command.MAGIC_BYTE_HIGH);

        socket.getOutputStream().write(Command.MAGIC_BYTE_LOW);

        int serialNumber = 1;
        socket.getOutputStream().write((byte)0x00);
        socket.getOutputStream().write((byte)0x00);
        socket.getOutputStream().write((byte)0x00);
        socket.getOutputStream().write((byte)0x00);
        socket.getOutputStream().write((byte)0x00);
        socket.getOutputStream().write((byte)0x00);
        socket.getOutputStream().write((byte)0x00);
        socket.getOutputStream().write((byte)serialNumber);

        socket.getOutputStream().write(Command.REQUEST);

        socket.getOutputStream().write(Command.JSON_SERIALIZATION);

        byte[] payloadLengthBytes = new byte[4];
        int payloadLength = payload.length;
        ByteBuffer.wrap(payloadLengthBytes).order(ByteOrder.BIG_ENDIAN).putInt(payloadLength);
        socket.getOutputStream().write(payloadLengthBytes);

        socket.getOutputStream().write(payload);

        //
        byte[] magicBytes = new byte[2];
        socket.getInputStream().read(magicBytes);
        Assert.assertEquals(Command.MAGIC_BYTE_HIGH, magicBytes[0]);
        Assert.assertEquals(Command.MAGIC_BYTE_LOW, magicBytes[1]);

        byte[] serialnumberBytes = new byte[8];
        socket.getInputStream().read(serialnumberBytes);
        Assert.assertEquals(1, serialnumberBytes[7]);

        byte commandType = (byte)socket.getInputStream().read();
        Assert.assertEquals(Command.RESPONSE, commandType);

        byte serializationType = (byte)socket.getInputStream().read();
        Assert.assertEquals(Command.JSON_SERIALIZATION, serializationType);

        byte[] responsePayloadLengthBytes = new byte[4];
        socket.getInputStream().read(responsePayloadLengthBytes);
        int responsePayloadLength = ByteBuffer.wrap(responsePayloadLengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        Assert.assertTrue(responsePayloadLength > 0);

        byte[] responsePayloadBytes = new byte[responsePayloadLength];
        socket.getInputStream().read(responsePayloadBytes);
        String s = new String(responsePayloadBytes);
        System.out.println(s);

        socket.close();

        server.stop();

    }
}
