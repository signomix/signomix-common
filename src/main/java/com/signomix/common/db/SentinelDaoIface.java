package com.signomix.common.db;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.signomix.common.iot.sentinel.SentinelConfig;

import io.agroal.api.AgroalDataSource;

public interface SentinelDaoIface {
    public void setDatasource(AgroalDataSource ds);

    public void backupDb() throws IotDatabaseException;

    public void createStructure() throws IotDatabaseException;
    public long addConfig(SentinelConfig config) throws IotDatabaseException;
    public void addDevice(long configId, String deviceEui, String channelMapping) throws IotDatabaseException;
    public void removeDevice(String deviceEui) throws IotDatabaseException;
    public void removeConfigDevice(long configId, String deviceEui) throws IotDatabaseException;
    public void removeDevices(long configId) throws IotDatabaseException;
    public Map<String,String> getDevices(long configId) throws IotDatabaseException;
    public void updateConfig(SentinelConfig config) throws IotDatabaseException;
    public void removeConfig(long id) throws IotDatabaseException;
    public SentinelConfig getConfig(long id) throws IotDatabaseException;
    public List<SentinelConfig> getConfigs(String userId, int limit, int offset) throws IotDatabaseException;
    public List<SentinelConfig> getOrganizationConfigs(long organizationId, int limit, int offset) throws IotDatabaseException;
    public List<SentinelConfig> getConfigsByDevice(String deviceEui, int limit, int offset) throws IotDatabaseException;
    public Map<String,Map<String,String>> getDeviceChannelsByConfigId(long configId) throws IotDatabaseException;
    public Map<String,Map<String,String>> getDeviceChannelsByConfigAndEui(long configId, String eui) throws IotDatabaseException;
    public List<List> getLastValuesByConfigId(long sentinelConfigId) throws IotDatabaseException;
    public List<List> getLastValuesByDeviceEui(String deviceEui) throws IotDatabaseException;
    public List<List> getLastValuesOfDevices(Set<String> euis, long secondsBack) throws IotDatabaseException;
    public void addSentinelEvent(long configId, String deviceEui, int level, String message_pl, String message_en) throws IotDatabaseException;
    public int getSentinelStatus(long configId) throws IotDatabaseException;
    public List<SentinelConfig> getConfigsByTag(String tagName, String tagValue, int limit, int offset) throws IotDatabaseException;
    public List<SentinelConfig> getConfigsByGroup(String groupName, int limit, int offset) throws IotDatabaseException;
}