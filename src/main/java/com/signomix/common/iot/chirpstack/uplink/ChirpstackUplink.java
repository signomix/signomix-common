package com.signomix.common.iot.chirpstack.uplink;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ChirpstackUplink {

    public String deduplicationId;
    public String time;
    @JsonProperty("deviceInfo")
    public DeviceInfo deviceinfo;
    public String devAddr;
    public long dr;
    public long fPort;
    public long fCnt;
    public String data;
    public List<RxInfo> rxInfo;
    @JsonProperty("txInfo")
    public TxInfo txInfo;
    public String objectJSON;
    public HashMap<String, Object> object;

}
