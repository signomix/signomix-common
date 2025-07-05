package com.signomix.common.iot.tts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GatewayIds {

    @JsonProperty("gateway_id")
    private String gatewayId;

    @JsonProperty("eui")
    private String eui;

    // Gettery, Settery i toString()
    public String getGatewayId() {
        return gatewayId;
    }

    public void setGatewayId(String gatewayId) {
        this.gatewayId = gatewayId;
    }

    public String getEui() {
        return eui;
    }

    public void setEui(String eui) {
        this.eui = eui;
    }

    @Override
    public String toString() {
        return "GatewayIds{" +
                "gatewayId='" + gatewayId + '\'' +
                ", eui='" + eui + '\'' +
                '}';
    }
}
