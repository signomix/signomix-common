package com.signomix.common;

import java.util.UUID;

public class MessageEnvelope {

    public static final String GENERAL = "GENERAL";
    public static final String INFO = "INFO";
    public static final String WARNING = "WARNING";
    public static final String ALERT = "ALERT";
    public static final String DEVICE_LOST = "DEVICE_LOST";
    public static final String PLATFORM_DEVICE_LIMIT_EXCEEDED = "PLATFORM_DEVICE_LIMIT_EXCEEDED";
    public static final String ADMIN_EMAIL = "ADMIN_EMAIL";
    public static final String DIRECT_EMAIL = "DIRECT_EMAIL";
    public static final String MAILING = "MAILING";

    public UUID uuid;
    public String type;
    public String eui;
    public String subject;
    public String message;
    public String userIds; // user uid list separated by ";"
    public User user;

    public MessageEnvelope() {
        uuid = UUID.randomUUID();
    }
}
