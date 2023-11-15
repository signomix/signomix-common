package com.signomix.common.iot.sentinel;

import java.util.List;

public class SentinelConfig {
    public static final int TYPE_DEVICE = 0;
    public static final int TYPE_GROUP = 1;
    public static final int TYPE_TAG = 2;

    public Long id;
    public String name;
    public boolean active;
    public Long userId;
    public Long organizationId;
    public int type;
    public String deviceEui;
    public String groupEui;
    public String tagName;
    public String tagValue;
    public int alertLevel;
    public String alertMessage;
    public boolean everyTime;
    public boolean conditionOkMessage;
    public List<AlarmCondition> conditions;
    public String team;
    public String administrators;

    public SentinelConfig() {
        conditions=new java.util.ArrayList<>();
    }
}
