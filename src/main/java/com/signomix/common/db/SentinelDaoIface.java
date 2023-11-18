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
    public void removeDevice(long configId, String deviceEui) throws IotDatabaseException;
    public Map<String,String> getDevices(long configId) throws IotDatabaseException;
    public void updateConfig(SentinelConfig config) throws IotDatabaseException;
    public void removeConfig(long id) throws IotDatabaseException;
    public SentinelConfig getConfig(long id) throws IotDatabaseException;
    public List<SentinelConfig> getConfigs(String userId, int limit, int offset) throws IotDatabaseException;
    public List<SentinelConfig> getOrganizationConfigs(long organizationId, int limit, int offset) throws IotDatabaseException;
    public List<SentinelConfig> getConfigsByDevice(String deviceEui, int limit, int offset) throws IotDatabaseException;
    public Map<String,Map<String,String>> getDevicesByConfigId(long configId, int limit, int offset) throws IotDatabaseException;
    public List<List> getLastValuesByConfigId(long sentinelConfigId) throws IotDatabaseException;
    public void addSentinelEvent(long configId, String deviceEui, int level, String message_pl, String message_en) throws IotDatabaseException;
}
