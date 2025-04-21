package com.signomix.common;

import java.beans.Transient;

public class Organization {
    public Integer id;
    public String code;
    public String name;
    public String description;
    public String configuration;
    private Integer numberOfTenants = 0;
    public Boolean locked = false;

    public Organization() {
        this.id = 0;
        this.name = "";
        this.code = "";
        this.description = "";
        this.configuration = "";
        this.numberOfTenants = 0;
        this.locked = false;
    }

    public Organization(Integer id, String code, String name, String description, String configuration) {
        this.id = id;
        this.name = name;
        this.code = code;
        this.description = description;
        this.configuration = configuration;
    }

    public Organization(Integer id, String code, String name, String description, String configuration, Boolean locked) {
        this.id = id;
        this.name = name;
        this.code = code;
        this.description = description;
        this.configuration = configuration;
        this.locked = locked;
    }



    public Integer getNumberOfTenants() {
        return numberOfTenants;
    }

    @Transient
    public void setNumberOfTenants(Integer numberOfTenants) {
        this.numberOfTenants = numberOfTenants;
    }
}
