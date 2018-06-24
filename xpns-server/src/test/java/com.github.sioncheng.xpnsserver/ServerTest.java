package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import com.github.sioncheng.xpns.common.client.SessionManager;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import okhttp3.*;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ServerTest {

    @Test
    public void testHandleClient() throws Exception {

        final String acid = "320000000001";

        SessionManager sessionManager = this.createSessionManager();

        XpnsServer xpnsServer = new XpnsServer(this.createXpnsServerConfig(),
                sessionManager,
                this.createNotificationManager());
        boolean result = xpnsServer.start();
        Assert.assertTrue(result);

        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(8080));

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("acid", acid);
        jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.LOGIN_CODE);

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

        JSONObject jsonObject1 = JSON.parseObject(s);
        Assert.assertEquals(JsonCommand.LOGON_CODE, jsonObject1.getInteger(JsonCommand.COMMAND_CODE).intValue());

        Thread.sleep(200);

        SessionInfo sessionInfo = sessionManager.getClient(acid);
        Assert.assertNotNull(sessionInfo);

        final MediaType jsonMediaType = MediaType.parse("application/json; charset=utf-8");

        JSONObject getClientReq = new JSONObject();
        getClientReq.put("acid", acid);
        RequestBody requestBody = RequestBody.create(jsonMediaType, getClientReq.toJSONString());

        Request request = new Request.Builder().url("http://localhost:9090/client")
                .post(requestBody)
                .addHeader("Content-Type", "application/json")
                .build();

        OkHttpClient okHttpClient = new OkHttpClient();

        Response response = okHttpClient.newCall(request).execute();
        String body = response.body().string();
        response.close();

        JSONObject responseObject = JSON.parseObject(body);
        JSONObject sessionInfoJson = responseObject.getJSONObject("sessionInfo");
        Assert.assertNotNull(sessionInfoJson);

        Notification notification = new Notification();
        notification.setTo(acid);
        notification.setTitle("title");
        notification.setBody("body");
        notification.setExt(responseObject);

        requestBody = RequestBody.create(jsonMediaType, notification.toJSONObject().toJSONString());
        request = new Request.Builder().url("http://localhost:9090/notification")
                .post(requestBody)
                .addHeader("Content-Type", "application/json")
                .build();

        response = okHttpClient.newCall(request).execute();
        body = response.body().string();
        response.close();

        responseObject = JSON.parseObject(body);
        Assert.assertEquals("ok", responseObject.getString("result"));

        socket.getInputStream().read(magicBytes);
        Assert.assertEquals(Command.MAGIC_BYTE_HIGH, magicBytes[0]);
        Assert.assertEquals(Command.MAGIC_BYTE_LOW, magicBytes[1]);

        serialnumberBytes = new byte[8];
        socket.getInputStream().read(serialnumberBytes);
        Assert.assertEquals(1, serialnumberBytes[7]);

        commandType = (byte)socket.getInputStream().read();
        Assert.assertEquals(Command.REQUEST, commandType);

        serializationType = (byte)socket.getInputStream().read();
        Assert.assertEquals(Command.JSON_SERIALIZATION, serializationType);

        responsePayloadLengthBytes = new byte[4];
        socket.getInputStream().read(responsePayloadLengthBytes);
        responsePayloadLength = ByteBuffer.wrap(responsePayloadLengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        Assert.assertTrue(responsePayloadLength > 0);

        responsePayloadBytes = new byte[responsePayloadLength];
        socket.getInputStream().read(responsePayloadBytes);
        s = new String(responsePayloadBytes);
        System.out.println(s);

        socket.close();

        xpnsServer.stop();

        Thread.sleep(200);

        SessionInfo sessionInfo1 = sessionManager.getClient("320000000001");
        Assert.assertNull(sessionInfo1);

    }

    private XpnsServerConfig createXpnsServerConfig() {

        XpnsServerConfig xpnsServerConfig = new XpnsServerConfig();
        xpnsServerConfig.setClientPort(8080);
        xpnsServerConfig.setApiServer("localhost");
        xpnsServerConfig.setApiPort(9090);
        xpnsServerConfig.setMaxClients(10);
        xpnsServerConfig.setNettyEventLoopGroupThreadsForClient(4);
        xpnsServerConfig.setNettyEventLoopGroupThreadsForApi(2);


        return xpnsServerConfig;
    }

    private KafkaNotificationManager createNotificationManager() {

        KafkaNotificationConsumerConfig consumerConfig = new KafkaNotificationConsumerConfig();
        consumerConfig.setAutoCommitIntervalMS(1000);
        consumerConfig.setBootstrapServer("localhost:9092");
        consumerConfig.setEnableAutoCommit(true);
        consumerConfig.setGroupId("xpns-server");
        consumerConfig.setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.setSessionTimeoutMS(30000);

        KafkaNotificationManager notificationManager = new KafkaNotificationManager(consumerConfig);

        return notificationManager;
    }

    private SessionManager createSessionManager() {

        RedisSessionManager redisSessionManager = new RedisSessionManager("localhost", 6379);

        return redisSessionManager;
    }
}
