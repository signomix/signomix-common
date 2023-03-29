package com.signomix.common.db;

import io.agroal.api.AgroalDataSource;

public interface CmsDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void backupDb() throws IotDatabaseException;
    public void createStructure() throws IotDatabaseException;
}
