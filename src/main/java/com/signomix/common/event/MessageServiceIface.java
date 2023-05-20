package com.signomix.common.event;

import com.signomix.common.EventEnvelope;
import com.signomix.common.MessageEnvelope;

public interface MessageServiceIface {

    public void sendEvent(EventEnvelope wrapper);

    public void sendNotification(IotEvent event);

    public void sendData(IotEvent event);

    public void sendCommand(IotEvent event);

    public void sendDeviceEvent(EventEnvelope wrapper);

    public void sendDbEvent(EventEnvelope wrapper);

    public void sendAdminEmail(MessageEnvelope wrapper);

    public void sendErrorInfo(EventEnvelope wrapper);

}