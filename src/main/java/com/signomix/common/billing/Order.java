package com.signomix.common.billing;

import java.sql.Timestamp;

public class Order {

    public String id;
    public String uid;
    public Long userNumber;
    public Integer accountType;
    public Integer targetType;
    public String name;
    public String surname;
    public String email;
    public String address;
    public String city;
    public String zip;
    public String country;
    public String taxNumber;
    public String companyName;
    public Boolean yearly;
    public String currency;
    public Timestamp firstPaidAt;
    public Timestamp nextPaymentAt;
    public String paymentMethod;
    public Timestamp createdAt;
    public Double price;
    public String tax;
    public Double vatValue;
    public Double total;


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
