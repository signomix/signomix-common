package com.signomix.common.iot.tts;

import java.sql.Timestamp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UplinkMessage {

    @JsonProperty("end_device_ids")
    private EndDeviceIds endDeviceIds;

    @JsonProperty("uplink_message")
    private UplinkPayload uplinkPayload;

    @JsonProperty("received_at")
    private Timestamp receivedAt;

    // Gettery i Settery

    public Timestamp getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Timestamp receivedAt) {
        this.receivedAt = receivedAt;
    }
    
    public EndDeviceIds getEndDeviceIds() {
        return endDeviceIds;
    }

    public void setEndDeviceIds(EndDeviceIds endDeviceIds) {
        this.endDeviceIds = endDeviceIds;
    }

    public UplinkPayload getUplinkPayload() {
        return uplinkPayload;
    }

    public void setUplinkPayload(UplinkPayload uplinkPayload) {
        this.uplinkPayload = uplinkPayload;
    }

    @Override
    public String toString() {
        return "UplinkMessage{" +
                "endDeviceIds=" + endDeviceIds +
                ", uplinkPayload=" + uplinkPayload +
                '}';
    }
}