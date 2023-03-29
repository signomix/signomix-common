package com.signomix.common.db;

import io.agroal.api.AgroalDataSource;

public interface ShortenerDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void putUrl(String path, String target)  throws IotDatabaseException;
    public String getTarget(String path)  throws IotDatabaseException;
    public void removeUrl(String target)  throws IotDatabaseException;
    public void backupDb() throws IotDatabaseException;
    public void createStructure() throws IotDatabaseException;
}
