package com.signomix.common.proprietary;

public interface AccountTypesIface {
    public Double getMonthlyPrice(int type, String currency);
    public Double getYearlyPrice(int type, String currency);
    public Boolean paidVersionAvailable();
}
