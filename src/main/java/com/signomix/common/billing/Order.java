package com.signomix.common.billing;

import java.sql.Timestamp;

public class Order {

    public String id;
    public String uid;
    public Long userNumber;
    public Integer accountType; // actual account type
    public Integer targetType; // ordered account type
    public String name; // user name
    public String surname; // user surname
    public String email;
    public String address;
    public String city;
    public String zip;
    public String country;
    public String taxNumber; // company tax number
    public String companyName;
    public Boolean yearly; // true = yearly, false = monthly
    public String currency; // EUR, USD, ...
    public Timestamp firstPaidAt;
    public Timestamp nextPaymentAt;
    public String paymentMethod;
    public Timestamp createdAt;
    public String serviceName;
    public Double price; // price without VAT
    public String tax; // VAT rate
    public Double vatValue; // VAT value
    public Double total; // price + VAT


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
