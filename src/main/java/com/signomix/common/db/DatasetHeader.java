package com.signomix.common.db;

import java.util.ArrayList;

/**
 * A dataset header is a list of column names.
 */
public class DatasetHeader {
    public ArrayList<String> columns;
    public String name;

    public DatasetHeader() {
        columns = new ArrayList<String>();
    }
    
    public DatasetHeader(String name) {
        this.name = name;
        columns = new ArrayList<String>();
    }

}
