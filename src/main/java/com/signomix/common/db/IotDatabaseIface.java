package com.signomix.common.db;

import java.util.ArrayList;
import java.util.List;

import com.signomix.common.User;
import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.Alert;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.DeviceGroup;
import com.signomix.common.iot.DeviceTemplate;
import com.signomix.common.iot.virtual.VirtualData;

import io.agroal.api.AgroalDataSource;

public interface IotDatabaseIface {
    public void backupDb() throws IotDatabaseException;
    public void createStructure() throws IotDatabaseException;
    public void setDatasource(AgroalDataSource ds);
    public void setQueryResultsLimit(int limit);

    public List<List> getValues2(String userID, String deviceEUI, String dataQuery) throws IotDatabaseException;

    public List<List> getValues(String userID, String deviceID,String dataQuery)  throws IotDatabaseException;
    public List<List<List>> getGroupValues(String userID, long organizationId, String groupEUI, String[] channelNames, String dataQuery) throws IotDatabaseException;
    public List<List<List>> getGroupLastValues(String userID, long organizationId, String groupEUI, String[] channelNames, long secondsBack) throws IotDatabaseException;
    public void putData(Device device, ArrayList<ChannelData> list) throws IotDatabaseException;
    public void putVirtualData(Device device, VirtualData data) throws IotDatabaseException;
    
    //GROUPS
    public DeviceGroup getGroup(String groupEUI) throws IotDatabaseException;
    public List<DeviceGroup> getOrganizationGroups(long organizationId, int limit, int offset) throws IotDatabaseException;
    public List<DeviceGroup> getUserGroups(String userID, int limit, int offset) throws IotDatabaseException;
    public void updateGroup(DeviceGroup group) throws IotDatabaseException;
    public void createGroup(DeviceGroup group) throws IotDatabaseException;
    public void deleteGroup(String groupEUI) throws IotDatabaseException;
    //DEVICES
    public List<Device> getUserDevices(User user, boolean withStatus, Integer limit, Integer offset) throws IotDatabaseException;
    public List<Device> getOrganizationDevices(long organizationId, boolean withStatus, Integer limit, Integer offset) throws IotDatabaseException;
    public Device getDevice(User user, String deviceEUI, boolean withShared, boolean withStatus) throws IotDatabaseException;
    public Device getDevice(String eui, boolean withStatus) throws IotDatabaseException;
    //public Device getDevice(String userID, String deviceEUI, boolean withShared, boolean withStatus) throws IotDatabaseException;
    //public Device getDevice(User user, boolean withStatus) throws IotDatabaseException;
    public void deleteDevice(User user, String deviceEUI) throws IotDatabaseException;
    public void updateDevice(User user, Device device) throws IotDatabaseException;
    public void createDevice(User user, Device device) throws IotDatabaseException;
    public List<Device> getInactiveDevices() throws IotDatabaseException;
    //data migration
    public List<Device> getAllDevices() throws IotDatabaseException;
    public void addDevice(Device device) throws IotDatabaseException;
    public List<DeviceTemplate> getAllDeviceTemplates() throws IotDatabaseException;
    public void addDeviceTemplate(DeviceTemplate device) throws IotDatabaseException;

    public List<String> getDeviceChannels(String deviceEUI) throws IotDatabaseException;//public List<List<List>> getGroupValues(String userID, String deviceID,String dataQuery)  throws IotDatabaseException;
    public void clearDeviceData(String deviceEUI) throws IotDatabaseException;
    public void updateDeviceChannels(String deviceEUI, String channels) throws IotDatabaseException;
    public void updateDeviceStatus(String eui, long transmissionInterval, Double newStatus, int newAlertStatus) throws IotDatabaseException;
    
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
    public List<Alert> getAlerts(String userID, boolean descending) throws IotDatabaseException;
    public Long getAlertsCount(String userID) throws IotDatabaseException;
    public List<Alert> getAlerts(String userID, int limit, int offset, boolean descending) throws IotDatabaseException;
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

    //system parameters
    public long getParameterValue(String name, long accountType) throws IotDatabaseException;
    public String getParameterTextValue(String name, long accountType) throws IotDatabaseException;
    public void setParameter(String name, long accountType, long value, String text) throws IotDatabaseException;
    //system features
    public boolean isFeatureEnabled(String name, long accountType) throws IotDatabaseException;
    public void setFeature(String name, long accountType, boolean enabled) throws IotDatabaseException;

    //favourites
    public void addFavouriteDevice(String userID, String eui) throws IotDatabaseException;
    public void removeFavouriteDevices(String userID, String eui) throws IotDatabaseException;
    public List<Device> getFavouriteDevices(String userID) throws IotDatabaseException;

}

