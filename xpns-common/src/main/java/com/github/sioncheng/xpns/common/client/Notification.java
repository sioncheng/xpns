package com.github.sioncheng.xpns.common.client;

import com.alibaba.fastjson.JSONObject;

public class Notification {

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

    public JSONObject toJSONObject() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("uniqId", uniqId);
        jsonObject.put("to", to);
        jsonObject.put("title", title);
        jsonObject.put("body", body);
        jsonObject.put("ext", ext);

        return jsonObject;
    }

    public void fromJSONObject(JSONObject jsonObject) {
        this.setUniqId(jsonObject.getString("uniqId"));
        this.setTo(jsonObject.getString("to"));
        this.setTitle(jsonObject.getString("title"));
        this.setBody(jsonObject.getString("body"));
        this.setExt(jsonObject.getJSONObject("ext"));
    }

    private String uniqId;

    private String to;

    private String title;

    private String body;

    private JSONObject ext;
}
