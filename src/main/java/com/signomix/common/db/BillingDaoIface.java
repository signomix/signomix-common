package com.signomix.common.db;

import com.signomix.common.billing.Order;

import io.agroal.api.AgroalDataSource;

public interface BillingDaoIface {

    public void setDatasource(AgroalDataSource dataSource);
    public void createStructure() throws IotDatabaseException;
    public void backupDb() throws IotDatabaseException;
    /**
     * Get the last order number for the given month and year.
     * @param month
     * @param year
     * @return
     */
    public int getOrderCount(int month, int year) throws IotDatabaseException;
    public Order createOrder(Order order) throws IotDatabaseException;
    public void updateOrder(Order order) throws IotDatabaseException;
    public Order getOrder(String id) throws IotDatabaseException;    
    public void setServicePoints(String userId, long points) throws IotDatabaseException; //set service points
    public void registerServicePoints(String userId, long points) throws IotDatabaseException; //register service points (plus or minus)
    public long getServicePoints(String userId) throws IotDatabaseException; //get current service point balance
}
