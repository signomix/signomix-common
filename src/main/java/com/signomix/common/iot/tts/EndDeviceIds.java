package com.signomix.common.iot.tts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EndDeviceIds {

    @JsonProperty("device_id")
    private String deviceId;

    @JsonProperty("dev_eui")
    private String deviceEui;

    @JsonProperty("application_ids")
    private ApplicationIds applicationIds;

    // Gettery i Settery
    public String getDeviceEui() {
        return deviceEui;
    }

    public void setDeviceEui(String deviceEui) {
        this.deviceEui = deviceEui;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public ApplicationIds getApplicationIds() {
        return applicationIds;
    }

    public void setApplicationIds(ApplicationIds applicationIds) {
        this.applicationIds = applicationIds;
    }

    @Override
    public String toString() {
        return "EndDeviceIds{" +
                "deviceId='" + deviceId + '\'' +
                ", applicationIds=" + applicationIds +
                '}';
    }
}