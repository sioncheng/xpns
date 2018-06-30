package com.github.sioncheng.xpns.soa;

import com.alibaba.fastjson.JSONObject;

public class NotificationRequest {

    public NotificationRequest() {}

    public String getUniqId() {
        return uniqId;
    }

    public void setUniqId(String uniqId) {
        this.uniqId = uniqId;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public JSONObject getExt() {
        return ext;
    }

    public void setExt(JSONObject ext) {
        this.ext = ext;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    private String uniqId;

    private String to;

    private String title;

    private String body;

    private JSONObject ext;

    private int ttl;
}

