package com.signomix.common.db;

import java.util.ArrayList;

public class GroupDataset {
    /**
     * The number of datasets (datasources) in the group
     */
    public Long size;
    /**
     * EUI of the group dataset (optional)
     */
    public String eui;
    /**
     * Name of the group dataset - this could be the name of the group (if eui is not null)
     * or the name of the dataset not related to eui (if eui is null)
     */
    public String name;
    public ArrayList<Dataset> datasources;

    public GroupDataset() {
        datasources = new ArrayList<Dataset>();
    }

    public GroupDataset(String eui) {
        this.eui = eui;
        datasources = new ArrayList<Dataset>();
    }
}
