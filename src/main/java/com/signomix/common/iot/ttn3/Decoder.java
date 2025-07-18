
package com.signomix.common.iot.ttn3;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;

import com.cedarsoftware.util.io.JsonReader;
import com.cedarsoftware.util.io.JsonWriter;

/**
 *
 * @author greg
 */
public class Decoder {

    private static final Logger logger = Logger.getLogger(Decoder.class);

    /*
     * public String deviceEui;
     * public String deviceId;
     * public String timestampStr1;
     * public String timestampStr2;
     * public Long fPort;
     * public String frmPayload;
     * public Map decodedPayload;
     * public Double latitude;
     * public Double longitude;
     * public Double altitude;
     * public Object[] rxMetadata;
     * 
     */
    /*
     * public String getDeviceEUI();
     * public String getDeviceID();
     * public String getPayload();
     * public String[] getPayloadFieldNames();
     * //public long getLongValue(String fieldName, int multiplier);
     * public Instant getTimeField();
     * public long getTimestamp();
     * public long getReceivedPackageTimestamp(); // timestamp from data object
     * metadata
     * public Double getDoubleValue(String fieldName);
     * public String getStringValue(String fieldName);
     * public Double getLatitude();
     * public Double getLongitude();
     * public Double getAltitude();
     */

    public static TtnData3 decode(String json) {
        if (logger.isDebugEnabled()) {
            logger.debug("Decoding TTN3 data: " + json);
        }
        String rxMetadataJson;
        TtnData3 data = new TtnData3();
        Map map = JsonReader.jsonToMaps(json);
        if (map == null || map.isEmpty()) {
            logger.error("Received empty or null data");
            logger.info("Malformed JSON: " + json);
            return null;
        }
        if (map.get("data") != null) {
            map = (Map) map.get("data");
            if (map == null || map.isEmpty()) {
                logger.error("Received empty or null data object in data");
                logger.info("Malformed JSON: " + json);
                return null;
            }
        }

        data.deviceId = (String) ((Map) map.get("end_device_ids")).get("device_id");
        data.deviceEui = (String) ((Map) map.get("end_device_ids")).get("dev_eui");
        data.timestampStr1 = (String) map.get("received_at");
        Map uplinkMessage = (Map) map.get("uplink_message");
        data.fPort = (Long) uplinkMessage.get("f_port");
        data.fCounter = (Long) uplinkMessage.get("f_cnt");
        data.frmPayload = (String) uplinkMessage.get("frm_payload");
        data.decodedPayload = (Map) uplinkMessage.get("decoded_payload");
        data.rxMetadata = (List) uplinkMessage.get("rx_metadata");
        // data.timestampStr2 = (String) map.get("received_at");
        try {
            Instant instant = Instant.parse(data.timestampStr1);
            data.timestamp = Timestamp.from(instant).getTime();
        } catch (DateTimeParseException e) {
            logger.error("Error parsing timestamp: " + data.timestampStr1, e);
            data.timestamp = System.currentTimeMillis();
        }
        data.normalize();
        if (logger.isDebugEnabled()) {
            HashMap args = new HashMap();
            args.put(JsonWriter.PRETTY_PRINT, true);
            args.put(JsonWriter.TYPE, false);
            rxMetadataJson = JsonWriter.objectToJson(data.rxMetadata, args);
            logger.debug(rxMetadataJson);
        }
        return data;
    }

}
