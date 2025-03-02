package com.signomix.common.db;

import java.util.List;

import com.signomix.common.iot.sentinel.Signal;

import io.agroal.api.AgroalDataSource;

public interface SignalDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public AgroalDataSource getDataSource();
    public void backupDb() throws IotDatabaseException;
    public void createStructure() throws IotDatabaseException;
    public void saveSignal(Signal signal) throws IotDatabaseException;
    public Signal getSignalById(long id) throws IotDatabaseException;
    public void updateSignal(Signal signal) throws IotDatabaseException;
    public void deleteSignal(long id) throws IotDatabaseException;
    public void deleteSignals(String userId) throws IotDatabaseException;
    public List<Signal> getUserSignals(String userId, int limit, int offset) throws IotDatabaseException;
    public List<Signal> getOrganizationSignals(long organizationId, int limit, int offset) throws IotDatabaseException;
    public void archiveSignals(long checkpoint) throws IotDatabaseException;
    public void clearOldSignals(long checkpoint) throws IotDatabaseException;
    public void archiveUserSignals(long checkpoint) throws IotDatabaseException;
    public void clearOldUserSignals(long checkpoint) throws IotDatabaseException;
}
