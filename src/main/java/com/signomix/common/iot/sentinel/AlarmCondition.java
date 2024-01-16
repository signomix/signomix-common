package com.signomix.common.iot.sentinel;

public class AlarmCondition {
    public static final int CONDITION_EQUAL = 0;
    public static final int CONDITION_NOT_EQUAL = 2;
    public static final int CONDITION_GREATER = 1;
    public static final int CONDITION_LESS = -1;
    public static final int CONDITION_OPERATOR_AND = 3;
    public static final int CONDITION_OPERATOR_OR = 4;

    public Integer conditionOperator;
    public String measurement;
    public Integer condition1;
    public Double value1;
    public boolean orOperator = false; //deprecated
    public Integer logic; // 0 - none, 1- OR, 2 - AND
    public Integer condition2;
    public Double value2;
}
