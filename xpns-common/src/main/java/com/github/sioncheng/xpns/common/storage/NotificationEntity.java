package com.github.sioncheng.xpns.common.storage;

import com.github.sioncheng.xpns.common.client.Notification;

public class NotificationEntity {

    public static final int NEW = 0;
    public static final int DEQUEUE = 1;
    public static final int SENT = 2;
    public static final int OFFLINE = 3;
    public static final int RESEND = 4;
    public static final int UNDELIVER = 5;
    public static final int DELIVERED = 6;

    public NotificationEntity clone() {
        NotificationEntity notificationEntity = new NotificationEntity();
        notificationEntity.setNotification(this.getNotification());
        notificationEntity.setStatus(this.getStatus());
        notificationEntity.setStatusDateTime(this.getStatusDateTime());
        notificationEntity.setCreateDateTime(this.getCreateDateTime());
        notificationEntity.setTtl(this.getTtl());

        return notificationEntity;
    }

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

    public String getStatusDateTime() {
        return statusDateTime;
    }

    public void setStatusDateTime(String statusDateTime) {
        this.statusDateTime = statusDateTime;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    private Notification notification;

    private int status;

    private String statusDateTime;

    private String createDateTime;

    private int ttl;
}
