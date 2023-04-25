package com.signomix.common.event;

import com.signomix.common.EventEnvelope;

public interface MessageServiceIface {

    /*public void sendErrorInfo(IotEvent event) {
        EventEnvelope wrapper=new EventEnvelope();
        wrapper.type=EventEnvelope.ERROR;
        LOG.info("sending error to MQ");
        eventsEmitter.send(event);
    }*/
    void sendEvent(EventEnvelope wrapper);

    void sendNotification(IotEvent event);

    void sendData(IotEvent event);

    void sendCommand(IotEvent event);

}