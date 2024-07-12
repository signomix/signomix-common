package com.signomix.common.db;

import com.signomix.common.billing.Order;

import io.agroal.api.AgroalDataSource;

public interface QdbDaoIface {

    public void setDatasource(AgroalDataSource dataSource);
    public void createStructure() throws IotDatabaseException;   
    public void backupDb() throws IotDatabaseException;
    public void deletePartition(String partition, int monthsBack) throws IotDatabaseException;
}
