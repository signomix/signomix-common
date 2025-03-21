package com.signomix.common;

import java.beans.Transient;

public class Organization {
    public Integer id;
    public String code;
    public String name;
    public String description;
    public String configuration;
    private Integer numberOfTenants;

    public Organization() {
        this.id = 0;
        this.name = "";
        this.code = "";
        this.description = "";
        this.configuration = "";
    }

    public Organization(Integer id, String code, String name, String description, String configuration) {
        this.id = id;
        this.name = name;
        this.code = code;
        this.description = description;
        this.configuration = configuration;
    }


    public Integer getNumberOfTenants() {
        return numberOfTenants;
    }

    @Transient
    public void setNumberOfTenants(Integer numberOfTenants) {
        this.numberOfTenants = numberOfTenants;
    }
}
