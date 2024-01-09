package com.signomix.common.iot.sentinel;

import java.sql.Timestamp;

public class Signal {
    public static final int MIN_LEVEL = 0;
    public static final int MAX_LEVEL = 5;
    public Long id;
    public Timestamp createdAt;
    public Timestamp readAt; // when the signal was read by the user (API ACK call)
    public Timestamp sentAt; // when the signal was sent with external notification
    public Timestamp deliveredAt; // when the signal was ACK'ed by the external notification system
    public String userId;
    public Long organizationId;
    public Long sentinelConfigId;
    public String deviceEui;
    public int level = 0;
    public String subjectPl;
    public String subjectEn;
    public String messageEn;
    public String messagePl;
}
