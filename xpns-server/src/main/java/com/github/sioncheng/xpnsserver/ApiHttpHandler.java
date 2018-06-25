package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.AsciiString;

import java.util.UUID;

public class ApiHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> { // 1

    public ApiHttpHandler(XpnsServer xpnsServer) {
        this.xpnsServer = xpnsServer;
    }

    private AsciiString contentType = HttpHeaderValues.TEXT_PLAIN;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {

        if ("/notification".equals(msg.uri())) {
            handleNotification(ctx, msg);
        } else if("/client".equals(msg.uri())) {
            handleClient(ctx, msg);
        } else {
            handle404(ctx, msg);
        }



    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        ctx.flush(); // 4
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if(null != cause) cause.printStackTrace();
        if(null != ctx) ctx.close();
    }

    private void handleNotification(ChannelHandlerContext ctx, FullHttpRequest msg) {
        byte[] bytes = new byte[msg.content().readableBytes()];
        msg.content().readBytes(bytes);
        String s = new String(bytes);
        JSONObject jsonObject = JSON.parseObject(s);
        Notification notification = new Notification();
        notification.fromJSONObject(jsonObject);
        String uuid = (UUID.randomUUID().toString());
        notification.setUniqId(uuid);

        this.xpnsServer.notificationIn(notification);

        JSONObject responseJsonObject = new JSONObject();
        responseJsonObject.put("messageId", notification.getUniqId());
        responseJsonObject.put("result", "ok");

        this.response(ctx, responseJsonObject.toJSONString());
    }

    private void handleClient(ChannelHandlerContext ctx, FullHttpRequest msg) {

        byte[] bytes = new byte[msg.content().readableBytes()];
        msg.content().readBytes(bytes);
        String s = new String(bytes);
        JSONObject jsonObject = JSON.parseObject(s);

        String acid = jsonObject.getString("acid");
        SessionInfo sessionInfo = this.xpnsServer.getClient(acid);
        JSONObject responseJsonObject = new JSONObject();
        if (sessionInfo == null) {
            sessionInfo = new SessionInfo();
            responseJsonObject.put("result", "error");
        } else {
            responseJsonObject.put("result", "ok");
        }
        responseJsonObject.put("sessionInfo", sessionInfo);

        this.response(ctx, responseJsonObject.toJSONString());
    }

    private void handle404(ChannelHandlerContext ctx, FullHttpRequest msg) {
        response(ctx, "404");
    }

    private void response(ChannelHandlerContext ctx, String msg) {
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(msg.getBytes())); // 2

        HttpHeaders heads = response.headers();
        heads.add(HttpHeaderNames.CONTENT_TYPE, contentType + "; charset=UTF-8");
        heads.add(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes()); // 3
        heads.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        ctx.writeAndFlush(response);
    }

    private XpnsServer xpnsServer;
}