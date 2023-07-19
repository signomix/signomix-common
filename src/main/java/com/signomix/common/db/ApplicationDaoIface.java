package com.signomix.common.db;

import java.util.List;

import com.signomix.common.iot.Application;

import io.agroal.api.AgroalDataSource;

public interface ApplicationDaoIface {
    public void setDatasource(AgroalDataSource ds);

    public void backupDb() throws IotDatabaseException;

    public void createStructure() throws IotDatabaseException;
    public Application addApplication(Application application) throws IotDatabaseException;
    public void updateApplication(Application application) throws IotDatabaseException;
    public void removeApplication(long id) throws IotDatabaseException;
    public Application getApplication(long id) throws IotDatabaseException;
    public Application getApplication(String name) throws IotDatabaseException;
    public List<Application> getAllApplications() throws IotDatabaseException;
}
