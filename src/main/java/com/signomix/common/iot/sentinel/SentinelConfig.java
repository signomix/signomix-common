package com.signomix.common.iot.sentinel;

import java.util.List;

public class SentinelConfig {
    public static final int TYPE_DEVICE = 0;
    public static final int TYPE_GROUP = 1;
    public static final int TYPE_TAG = 2;

    public Long id;
    public String name;
    public boolean active;
    public String userId;
    public Long organizationId;
    public int type;
    public String deviceEui;
    public String groupEui;
    public String tagName;
    public String tagValue;
    public int alertLevel;
    public String alertMessage;
    public boolean everyTime;
    public boolean conditionOk;
    public String conditionOkMessage;
    public List<AlarmCondition> conditions=new java.util.ArrayList<>();
    public String team;
    public String administrators;
    public int timeShift;

/*     public SentinelConfig() {
        conditions=new java.util.ArrayList<>();
    } */
}
