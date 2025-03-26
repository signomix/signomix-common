package com.signomix.common.iot;

public class LastDataPair {
    public String eui;
    public Double value;
    public Double delta;

    public LastDataPair(String eui, Double value, Double delta) {
        this.eui = eui;
        this.value = value;
        this.delta = delta;
    }

    public String toString() {
        return eui + " " + value + " " + delta;
    }
}
