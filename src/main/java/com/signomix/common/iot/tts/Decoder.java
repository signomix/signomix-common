package com.signomix.common.iot.tts;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.iot.ttn3.TtnData3;

public class Decoder {
    // Logger for debugging purposes
    private static final Logger logger = Logger.getLogger(Decoder.class);

    /**
     * Decodes a JSON string into a TtnData3 object.
     *
     * @param json the JSON string to decode
     * @return a TtnData3 object containing the decoded data
     */
    public static TtnData3 decode(String json){
        // Log the JSON string for debugging
        if (logger.isDebugEnabled()) {
            logger.debug(json);
        }
        if (json == null || json.isEmpty()) {
            return null;
        }
        // Use ObjectMapper to parse the JSON string
        // and convert it into a TtnData3 object
        TtnData3 data = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try{
            UplinkMessage uplinkMessage = objectMapper.readValue(json, UplinkMessage.class);
            if(logger.isDebugEnabled()){
                logger.debug("UplinkMessage: " + uplinkMessage);
            }
            data = new TtnData3();
            data.deviceEui=uplinkMessage.getEndDeviceIds().getDeviceEui();
            data.deviceId=uplinkMessage.getEndDeviceIds().getDeviceId();
            data.fPort=Long.valueOf(uplinkMessage.getUplinkPayload().getfPort());
            data.fCounter=uplinkMessage.getUplinkPayload().getfCnt();
            data.frmPayload=uplinkMessage.getUplinkPayload().getFrmPayload();
            data.decodedPayload=uplinkMessage.getUplinkPayload().getDecodedPayload();
            data.rxMetadata=uplinkMessage.getUplinkPayload().getRxMetadata();
            data.rxMetadataJson = objectMapper.writeValueAsString(data.rxMetadata);
            data.timestamp= uplinkMessage.getUplinkPayload().getReceivedAt().getTime();
        }catch (Exception e){
            // Log the exception if an error occurs during decoding
            logger.error("Error decoding JSON: " + e.getMessage());
            // Log the stack trace for better debugging
            if (logger.isDebugEnabled()) {
                logger.error("Stack trace:", e);
            }
            return null;
        }
        return data;
    }

}
