package com.signomix.common.db;

import java.util.List;

import com.signomix.common.iot.Application;
import com.signomix.common.iot.sentry.SentinelConfig;

import io.agroal.api.AgroalDataSource;

public interface SentinelDaoIface {
    public void setDatasource(AgroalDataSource ds);

    public void backupDb() throws IotDatabaseException;

    public void createStructure() throws IotDatabaseException;
    public void addConfig(SentinelConfig config) throws IotDatabaseException;
    public void updateConfig(SentinelConfig config) throws IotDatabaseException;
    public void removeConfig(long id) throws IotDatabaseException;
    public SentinelConfig getConfig(long id) throws IotDatabaseException;
    public List<SentinelConfig> getConfigs(long userId, int limit, int offset) throws IotDatabaseException;
    public List<SentinelConfig> getOrganizationConfigs(long organizationId, int limit, int offset) throws IotDatabaseException;
    public List<SentinelConfig> getConfigsByDevice(String deviceEui, int limit, int offset) throws IotDatabaseException;
}
