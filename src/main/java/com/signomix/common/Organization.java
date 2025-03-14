package com.signomix.common;

public class Organization {
    public Integer id;
    public String code;
    public String name;
    public String description;
    public Integer numberOfTenants;
    public String configuration;

    public Organization(Integer id, String code, String name, String description, Integer numberOfTenants, String configuration) {
        this.id = id;
        this.name = name;
        this.code = code;
        this.description = description;
        this.numberOfTenants = numberOfTenants;
        this.configuration = configuration;
    }
}
