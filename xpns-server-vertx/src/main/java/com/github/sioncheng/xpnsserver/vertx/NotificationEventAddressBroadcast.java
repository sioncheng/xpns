package com.github.sioncheng.xpnsserver.vertx;

public class NotificationEventAddressBroadcast {

    public static final String EVENT_ADDRESS = "notification.event.address";

    public NotificationEventAddressBroadcast() {

    }

    public NotificationEventAddressBroadcast(boolean on, String acid, String eventAddress) {
        this.on = on;
        this.acid = acid;
        this.eventAddress = eventAddress;
    }

    public boolean isOn() {
        return on;
    }

    public void setOn(boolean on) {
        this.on = on;
    }

    public String getAcid() {
        return acid;
    }

    public void setAcid(String acid) {
        this.acid = acid;
    }

    public String getEventAddress() {
        return eventAddress;
    }

    public void setEventAddress(String eventAddress) {
        this.eventAddress = eventAddress;
    }

    private boolean on;

    private String acid;

    private String eventAddress;
}
