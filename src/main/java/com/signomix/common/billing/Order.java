package com.signomix.common.billing;

import java.sql.Timestamp;

public class Order {

    public String id;
    public Timestamp createdAt;
    public Boolean yearly;
    public Integer accountType;
    public Long userNumber;
    public Timestamp firstPaidAt;
    public Timestamp nextPaymentAt;

    public Order() {
        createdAt = new Timestamp(System.currentTimeMillis());
        firstPaidAt = null;
        nextPaymentAt = null;
        id = null;
        accountType = null;
        userNumber = null;
        yearly = null;
    }

}
