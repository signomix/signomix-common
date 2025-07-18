/**
 * Copyright (C) Grzegorz Skorupa 2018.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.common.iot.generic;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.jboss.logging.Logger;

import com.signomix.common.DateTool;
import com.signomix.common.db.IotDataIface;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.chirpstack.uplink.ChirpstackUplink;
import com.signomix.common.iot.ttn3.TtnData;
import com.signomix.common.iot.ttn3.TtnData3;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class IotData2 implements IotDataIface {

    public String applicationID;
    public String deviceId = null;
    public String dev_eui;
    public String authKey;
    public boolean authRequired = false;
    // public String callbackurl;
    public String gateway_eui;
    public String time;
    public ArrayList<Map> payload_fields;
    public String timestamp;
    public String clientname;
    public ArrayList<ChannelData> dataList = new ArrayList<>();
    public String payload = null;
    public String hexPayload = null;
    public Timestamp timestampUTC;
    public Long port = null;
    public Long counter = null;
    public ChirpstackUplink chirpstackUplink = null;
    public TtnData3 ttnUplink = null;

    private Logger logger = Logger.getLogger(IotData2.class.getName());

    public IotData2(long systemTimestamp) {
        this.timestampUTC = new Timestamp(systemTimestamp);
    }

    @Override
    public String getDeviceEUI() {
        return dev_eui;
    }

    @Override
    public String[] getPayloadFieldNames() {
        String[] names = new String[payload_fields.size()];
        for (int i = 0; i < payload_fields.size(); i++) {
            names[i] = (String) payload_fields.get(i).get("name");
        }
        return names;
    }

    @Override
    public Instant getTimeField() {
        try {
            return Instant.parse(time);
        } catch (NullPointerException | DateTimeParseException e) {
            // e.printStackTrace();
        }
        return null;
    }

    @Override
    public long getTimestamp() {
        if (null == timestampUTC) {
            return System.currentTimeMillis();
        }
        return timestampUTC.getTime();
    }

    @Override
    public Timestamp getTimestampUTC() {
        return timestampUTC;
    }

    @Override
    public Double getDoubleValue(String fieldName) {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

    @Override
    public String getStringValue(String fieldName) {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

    /**
     * @return the applicationID
     */
    public String getApplicationID() {
        return applicationID;
    }

    /**
     * @param applicationID the applicationID to set
     */
    public void setApplicationID(String applicationID) {
        this.applicationID = applicationID;
    }

    public void setTimestampUTC(long systemTimestamp) {
        if( timestampUTC!=null){
            return;
        }
        // timestamp
        try {
            timestampUTC = DateTool.parseTimestamp(timestamp, time, true);
        } catch (Exception e) {
            e.printStackTrace();
            timestampUTC = new Timestamp(systemTimestamp);
        }
    }

    @Override
    public void normalize() {
        if (this.gateway_eui != null) {
            this.gateway_eui = this.gateway_eui.toUpperCase();
        }
        if (this.dev_eui != null) {
            this.dev_eui = this.dev_eui.toUpperCase();
        }
        // Data channel names should be lowercase. We can fix user mistakes here.
        HashMap<String, Object> tempMap;
        if (null == payload_fields) {
            payload_fields = new ArrayList<>();
        }
        for (int i = 0; i < payload_fields.size(); i++) {
            tempMap = new HashMap<>();
            tempMap.put("name", ((String) payload_fields.get(i).get("name")).toLowerCase());
            tempMap.put("stringValue", "" + payload_fields.get(i).get("value"));
            try {
                tempMap.put("value", (Double) payload_fields.get(i).get("value"));
            } catch (ClassCastException e) {
                try {
                    tempMap.put("value", ((Long) payload_fields.get(i).get("value")).doubleValue());
                } catch (ClassCastException e1) {
                    try {
                        tempMap.put("value", ((Integer) payload_fields.get(i).get("value")).doubleValue());
                    } catch (ClassCastException e2) {
                        tempMap.put("value", (String) payload_fields.get(i).get("value"));
                    }
                }
            }
            payload_fields.set(i, tempMap);
        }
    }

    @Override
    public String getPayload() {
        return payload;
    }

    @Override
    public String getHexPayload() {
        return hexPayload;
    }

    @Override
    public long getReceivedPackageTimestamp() {
        return getTimestamp();
    }

    @Override
    public String getDeviceID() {
        return "";
    }

    @Override
    public Double getLatitude() {
        return null;
    }

    @Override
    public Double getLongitude() {
        return null;
    }

    @Override
    public Double getAltitude() {
        return null;
    }

    /**
     * @return the authKey
     */
    public String getAuthKey() {
        return authKey;
    }

    /**
     * @return the dataList
     */
    public ArrayList<ChannelData> getDataList() {
        return dataList;
    }

    /**
     * @param dataList the dataList to set
     */
    public void setDataList(ArrayList<ChannelData> dataList) {
        this.dataList = dataList;
    }

    public void prepareIotValues(long timestamp) {
        long ts = timestamp;
        if (this.timestampUTC != null) {
            ts = this.timestampUTC.getTime();
        }
        if (ts == 0) {
            ts = timestamp;
        }
        for (int i = 0; i < this.payload_fields.size(); i++) {
            ChannelData mval = new ChannelData();
            mval.setTimestamp(ts);
            mval.setDeviceEUI(this.getDeviceEUI());
            mval.setName((String) this.payload_fields.get(i).get("name"));
            try {
                mval.setValue((Double) this.payload_fields.get(i).get("value"));
            } catch (ClassCastException e) {
                mval.setValue((String) this.payload_fields.get(i).get("value"));
            }
            mval.setStringValue("" + this.payload_fields.get(i).get("stringValue"));
            if (this.getTimeField() != null) {
                mval.setTimestamp(this.getTimeField().toEpochMilli());
            } else {
                /* try {
                    // mval.setTimestamp(this.getTimestamp());
                    mval.setTimestamp(timestamp);
                    logger.info("TIMESTAMP from clock: " + mval.getName());
                } catch (Exception e) {
                    mval.setTimestamp(timestamp);
                } */
            }
            if (mval.getTimestamp() == 0) {
                mval.setTimestamp(timestamp);
                //logger.info("TIMESTAMP from clock: " + mval.getName());
            }
            //logger.info("TIMESTAMP : "+mval.getName()+" "+mval.getTimestamp());
            this.dataList.add(mval);
        }
    }

}
