package com.signomix.common.db;

import com.signomix.common.iot.Application;
import com.signomix.common.iot.ApplicationConfig;
import io.agroal.api.AgroalDataSource;
import java.util.List;

public interface ApplicationDaoIface {
    public void setDatasource(AgroalDataSource ds);

    public void backupDb() throws IotDatabaseException;

    public void createStructure() throws IotDatabaseException;
    public Application addApplication(Application application) throws IotDatabaseException;
    public void updateApplication(Application application) throws IotDatabaseException;
    public void removeApplication(int id) throws IotDatabaseException;
    public Application getApplication(int id) throws IotDatabaseException;
    public Application getApplication(int organizationId, String name) throws IotDatabaseException;
    public List<Application> getApplications(int limit, int offset) throws IotDatabaseException;
    public List<Application> getApplications(int organizationId, int limit, int offset) throws IotDatabaseException;
    //public ApplicationConfig getApplicationConfig(long applicationId) throws IotDatabaseException;
    //public void setApplicationConfig(long applicationId, ApplicationConfig config) throws IotDatabaseException;
}
