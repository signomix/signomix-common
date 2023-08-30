package com.signomix.common.db;

import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public interface AuthDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void createStructure() throws IotDatabaseException;
    public String getUser(String token);
    public String createSession(User user);
    public void removeSession(String token);
    public void clearExpiredTokens();
    public void backupDb() throws IotDatabaseException;
}
