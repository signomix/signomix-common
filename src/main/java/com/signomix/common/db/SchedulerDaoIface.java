package com.signomix.common.db;

import io.agroal.api.AgroalDataSource;

public interface SchedulerDaoIface {
    public void setDatasource(AgroalDataSource ds);

    public void createStructure() throws IotDatabaseException;

    public void backupDb() throws IotDatabaseException;
}
