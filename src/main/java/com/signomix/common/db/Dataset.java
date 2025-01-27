package com.signomix.common.db;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A dataset is a list of rows where each row is a list of numbers.
 */
public class Dataset {
    /** The total number of data rows */
    public Long size;
    /** The resulting data */
    public ArrayList<DatasetRow> data;
    /** Datasource EUI (optional) */
    public String eui;
    /** Datasource name - this could be the name of the datasource (if eui is not null) 
     *  or the name of the dataset not related to eui (if eui is null)
     */
    public String name;

    public Dataset() {
        data = new ArrayList<DatasetRow>();
    }

    public Dataset(String name) {
        this.name = name;
        data = new ArrayList<DatasetRow>();
    }
}
