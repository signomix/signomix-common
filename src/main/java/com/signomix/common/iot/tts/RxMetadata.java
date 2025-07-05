package com.signomix.common.iot.tts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RxMetadata {

    @JsonProperty("gateway_ids")
    private GatewayIds gatewayIds;

    @JsonProperty("rssi")
    private int rssi;

    @JsonProperty("snr")
    private double snr;

    @JsonProperty("location")
    private Location location;

    // Gettery, Settery i toString()
    public GatewayIds getGatewayIds() {
        return gatewayIds;
    }

    public void setGatewayIds(GatewayIds gatewayIds) {
        this.gatewayIds = gatewayIds;
    }

    public int getRssi() {
        return rssi;
    }

    public void setRssi(int rssi) {
        this.rssi = rssi;
    }

    public double getSnr() {
        return snr;
    }

    public void setSnr(double snr) {
        this.snr = snr;
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "RxMetadata{" +
                "gatewayIds=" + gatewayIds +
                ", rssi=" + rssi +
                ", snr=" + snr +
                ", location=" + location +
                '}';
    }
}
