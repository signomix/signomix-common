package com.signomix.common.db;

import io.agroal.api.AgroalDataSource;

public interface ReportDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void backupDb() throws IotDatabaseException;
    public void createStructure() throws IotDatabaseException;
    public boolean isAvailable(String className, Long userNumber, Integer organization, Integer tenant, String path) throws IotDatabaseException;
    public void saveReport(String className, Long userNumber, Integer organization, Integer tenant, String path) throws IotDatabaseException;
}
