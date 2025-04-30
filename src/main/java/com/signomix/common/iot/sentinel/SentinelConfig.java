package com.signomix.common.iot.sentinel;

import java.util.List;

public class SentinelConfig {
    public static final int TYPE_DEVICE = 0;
    public static final int TYPE_GROUP = 1;
    public static final int TYPE_TAG = 2;

    public static final int EVENT_TYPE_DATA = 0;
    public static final int EVENT_TYPE_COMMAND = 1;
    public static final int EVENT_TYPE_ANY = 2;
    public static final int REACTION_TYPE_ALERT = 0;
    //public static final int REACTION_TYPE_COMMAND = 1;
    //public static final int REACTION_TYPE_ALERT_AND_COMMAND = 2;

    public Long id;
    public String name;
    public boolean active;
    public String userId;
    public Long organizationId;
    public int type;    // target type: device, group, tag
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
    public double hysteresis;
    public boolean useScript;
    public String script;
    public String scriptLanguage;
    public boolean checkOthers = false;

    public Integer eventType = 0; // 0: data event, 1: command event, null: data event
    //public Integer reactionType = 0; // 0: send alert, 1: send command, 2: send alert and command, null: send alert

/*     public SentinelConfig() {
        conditions=new java.util.ArrayList<>();
    } */
}
