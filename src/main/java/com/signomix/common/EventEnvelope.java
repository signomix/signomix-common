package com.signomix.common;

import java.util.UUID;

public class EventEnvelope {

    public static final String DEFAULT = "DEFAULT";
    public static final String USER = "USER";
    public static final String DEVICE = "DEVICE";
    public static final String DASHBOARD = "DASHBOARD";
    public static final String DATA = "DATA";
    public static final String ORGANIZATION = "ORGANIZATION";
    public static final String GROUP = "GROUP";
    public static final String APPLICATION = "APPLICATION";
    public static final String ERROR = "ERROR";
    public static final String SYSTEM = "SYSTEM";

    public UUID uuid;
    public String type;
    //public String id;
    public String eui; 
    public String payload;
    public long timestamp;

    public EventEnvelope() {
        uuid = UUID.randomUUID();
        timestamp=System.currentTimeMillis();
    }
}
