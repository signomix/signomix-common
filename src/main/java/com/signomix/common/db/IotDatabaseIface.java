package com.signomix.common.db;

import java.util.ArrayList;
import java.util.List;

import com.signomix.common.User;
import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.virtual.VirtualData;

import io.agroal.api.AgroalDataSource;

public interface IotDatabaseIface {
    public void backupDb() throws IotDatabaseException;
    public void createStructure() throws IotDatabaseException;
    public void setDatasource(AgroalDataSource ds);
    public void setQueryResultsLimit(int limit);


    public List<List> getValues(String userID, String deviceID,String dataQuery)  throws IotDatabaseException;
    public List<List<List>> getValuesOfGroup(String userID, long organizationId, String groupEUI, String channelNames, long secondsBack) throws IotDatabaseException;
    public void putData(Device device, ArrayList<ChannelData> list) throws IotDatabaseException;
    public void putVirtualData(Device device, VirtualData data) throws IotDatabaseException;
    
    //DEVICES
    public List<Device> getUserDevices(User user, boolean withStatus) throws IotDatabaseException;
    //public Device getDevice(User user, boolean withStatus) throws IotDatabaseException;
    public Device getDevice(User user, String deviceEUI, boolean withShared, boolean withStatus) throws IotDatabaseException;
    public void deleteDevice(User user, String deviceEUI) throws IotDatabaseException;
    public void updateDevice(User user, Device device) throws IotDatabaseException;
    public void createDevice(User user, Device device) throws IotDatabaseException;

    public Device getDevice(String eui, boolean withStatus) throws IotDatabaseException;
        public Device getDevice(String userID, String deviceEUI, boolean withShared, boolean withStatus) throws IotDatabaseException;
    public List<String> getDeviceChannels(String deviceEUI) throws IotDatabaseException;//public List<List<List>> getGroupValues(String userID, String deviceID,String dataQuery)  throws IotDatabaseException;
    public void updateDeviceStatus(String eui, Double newStatus, int newAlertStatus) throws IotDatabaseException;

    
    public ChannelData getLastValue(String userID, String deviceID, String channel) throws IotDatabaseException;
    public List<List> getLastValues(String userID, String deviceEUI) throws IotDatabaseException;
    
    public IotEvent getFirstCommand(String deviceEUI) throws IotDatabaseException;
    public void removeCommand(long id) throws IotDatabaseException;
    public void putCommandLog(String deviceEUI, IotEvent command) throws IotDatabaseException;
    public void putDeviceCommand(String deviceEUI, IotEvent commandEvent) throws IotDatabaseException; 
    public long getMaxCommandId() throws IotDatabaseException;
    public long getMaxCommandId(String deviceEui) throws IotDatabaseException;
    public int getChannelIndex(String deviceEUI, String channel) throws IotDatabaseException;
    //notifications
    public void addAlert(IotEvent alert) throws IotDatabaseException;
    public List getAlerts(String userID, boolean descending) throws IotDatabaseException;
    public void removeAlert(long alertID) throws IotDatabaseException;
    public void removeAlerts(String userID) throws IotDatabaseException;
    public void removeAlerts(String userID, long checkpoint) throws IotDatabaseException;
    public void removeOutdatedAlerts(long checkpoint) throws IotDatabaseException;

    public ChannelData getAverageValue(String userID, String deviceID, String channel, int scope, Double newValue) throws IotDatabaseException;
    public ChannelData getMinimalValue(String userID, String deviceID, String channel, int scope, Double newValue) throws IotDatabaseException;
    public ChannelData getMaximalValue(String userID, String deviceID, String channel, int scope, Double newValue) throws IotDatabaseException;
    public ChannelData getSummaryValue(String userID, String deviceID, String channel, int scope, Double newValue) throws IotDatabaseException;

    public void addSmsLog(long id, boolean confirmed, String phone, String text) throws IotDatabaseException;
    public void removeOutdatedSmsLogs(long checkpoint) throws IotDatabaseException;
    public void setConfirmedSms(long id) throws IotDatabaseException;
    public List<Long> getUnconfirmedSms() throws IotDatabaseException;
    public void removeOldData() throws IotDatabaseException;
}
