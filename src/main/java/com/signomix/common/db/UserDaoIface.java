package com.signomix.common.db;

import java.util.List;

import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public interface UserDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public String getUser(String uid);
    public void removeNotConfirmed(long since);
    public List<User> getAll();
    public void backupDb() throws IotDatabaseException;
}
