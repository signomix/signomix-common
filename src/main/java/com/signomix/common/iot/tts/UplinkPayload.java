
package com.signomix.common.iot.tts;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UplinkPayload {

    @JsonProperty("f_port")
    private int fPort;

    @JsonProperty("f_cnt")
    private long fCnt;

    @JsonProperty("frm_payload")
    private String frmPayload;

    @JsonProperty("decoded_payload")
    private Map<String, Object> decodedPayload;

    @JsonProperty("rx_metadata")
    private List<RxMetadata> rxMetadata;

    @JsonProperty("received_at")
    private Timestamp receivedAt;

    // Gettery i Settery

    public Timestamp getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Timestamp receivedAt) {
        this.receivedAt = receivedAt;
    }

    public long getfCnt() {
        return fCnt;
    }

    public void setfCnt(long fCnt) {
        this.fCnt = fCnt;
    }

    public int getfPort() {
        return fPort;
    }

    public void setfPort(int fPort) {
        this.fPort = fPort;
    }

    public String getFrmPayload() {
        return frmPayload;
    }

    public void setFrmPayload(String frmPayload) {
        this.frmPayload = frmPayload;
    }

    public Map<String, Object> getDecodedPayload() {
        return decodedPayload;
    }

    public void setDecodedPayload(Map<String, Object> decodedPayload) {
        this.decodedPayload = decodedPayload;
    }

    public List<RxMetadata> getRxMetadata() {
        return rxMetadata;
    }

    public void setRxMetadata(List<RxMetadata> rxMetadata) {
        this.rxMetadata = rxMetadata;
    }

    @Override
    public String toString() {
        return "UplinkPayload{" +
                "fPort=" + fPort +
                ", fCnt=" + fCnt +
                ", frmPayload='" + frmPayload + '\'' +
                ", decodedPayload=" + decodedPayload +
                ", rxMetadata=" + rxMetadata + // <-- ZAKTUALIZOWANE
                '}';
    }
}