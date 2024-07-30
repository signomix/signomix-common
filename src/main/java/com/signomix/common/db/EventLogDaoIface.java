package com.signomix.common.db;

import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public interface EventLogDaoIface {

    public void setDatasource(AgroalDataSource dataSource);
    public void createStructure() throws IotDatabaseException;   
    public void backupDb() throws IotDatabaseException;
    public void deletePartition(String partition, int monthsBack) throws IotDatabaseException;
    public void saveLoginEvent(User user, String remoteAddress, int resultCode);
    public void saveLoginFailure(String login, String remoteAddress, int resultCode);
}
