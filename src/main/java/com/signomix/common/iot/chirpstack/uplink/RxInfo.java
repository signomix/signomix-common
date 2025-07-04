package com.signomix.common.iot.chirpstack.uplink;

import java.util.Map;

public class RxInfo {
    
    public String gatewayId;
    public long uplinkId;
    public long rssi;
    public double snr;
    public String context;
    public Map<String,String> metadata;

}
