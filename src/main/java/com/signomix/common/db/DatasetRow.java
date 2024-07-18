package com.signomix.common.db;

import java.util.ArrayList;

/**
 * This class represents a single set of measurement values at a given timestamp, 
 * received from a datasource.
 * The order of values in the list corresponds to the order of the columns in the DatasetHeader.
 */
public class DatasetRow {
    public Long timestamp;
    public ArrayList<Object> values;

    public DatasetRow() {
        values = new ArrayList<>();
    }

}
