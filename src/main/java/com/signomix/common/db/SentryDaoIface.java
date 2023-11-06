package com.signomix.common.db;

import java.util.List;

import com.signomix.common.iot.Application;
import com.signomix.common.iot.sentry.SentryConfig;

import io.agroal.api.AgroalDataSource;

public interface SentryDaoIface {
    public void setDatasource(AgroalDataSource ds);

    public void backupDb() throws IotDatabaseException;

    public void createStructure() throws IotDatabaseException;
    public void addConfig(SentryConfig config) throws IotDatabaseException;
    public void updateConfig(SentryConfig config) throws IotDatabaseException;
    public void removeConfig(long id) throws IotDatabaseException;
    public SentryConfig getConfig(long id) throws IotDatabaseException;
    public List<SentryConfig> getConfigs(long userId, int limit, int offset) throws IotDatabaseException;
    public List<SentryConfig> getOrganizationConfigs(long organizationId, int limit, int offset) throws IotDatabaseException;
}
