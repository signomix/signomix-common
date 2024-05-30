package com.signomix.common.billing;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class Order {

    public String id;
    public Timestamp createdAt;
    public Boolean yearly;
    public Integer accountType;
    public Long userNumber;

    public Order() {
        createdAt = new Timestamp(System.currentTimeMillis());
    }

    /**
     * Create order id from incremented counter plus current locale month and year
     * number.
     */
    private String buildId(Timestamp createdAt) {
        LocalDateTime localDateTime = createdAt.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        int month = localDateTime.getMonthValue();
        int year = localDateTime.getYear();
        int lastOrder = getOrderCount(month, year);
        // Now you can use month and year in your ID
        String id = lastOrder+"/"+month + "/" + year;
        return id;
    }

    /**
     * Get the last order number for the given month and year.
     */
    private int getOrderCount(int month, int year) {
        // This is just a placeholder
        return 0;
    }
}
