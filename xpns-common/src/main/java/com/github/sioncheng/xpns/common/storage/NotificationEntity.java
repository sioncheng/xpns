package com.github.sioncheng.xpns.common.storage;

import com.github.sioncheng.xpns.common.client.Notification;

public class NotificationEntity {

    public static final int NEW = 0;
    public static final int QUEUE = 1;
    public static final int SENT = 2;
    public static final int UNDELIVER = 3;
    public static final int DELIVERED = 4;
    public static final int OFFLINE = 5;


    public Notification getNotification() {
        return notification;
    }

    public void setNotification(Notification notification) {
        this.notification = notification;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getCreateDateTime() {
        return createDateTime;
    }

    public void setCreateDateTime(String createDateTime) {
        this.createDateTime = createDateTime;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    private Notification notification;

    private int status;

    private String createDateTime;

    private int ttl;
}