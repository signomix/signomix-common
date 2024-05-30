package com.signomix.common.db;

import com.signomix.common.billing.Order;

import io.agroal.api.AgroalDataSource;

public interface BillingDaoIface {

    public void setDatasource(AgroalDataSource dataSource);
    public void createStructure() throws IotDatabaseException;
    /**
     * Get the last order number for the given month and year.
     * @param month
     * @param year
     * @return
     */
    public int getOrderCount(int month, int year);
    public Order createOrder(Order order);
    
}
