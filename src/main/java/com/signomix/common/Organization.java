package com.signomix.common;

public class Organization {
    public Integer id;
    public String code;
    public String name;
    public String description;
    public Integer numberOfTenants;

    public Organization(Integer id, String code, String name, String description, Integer numberOfTenants) {
        this.id = id;
        this.name = name;
        this.code = code;
        this.description = description;
        this.numberOfTenants = numberOfTenants;
    }
}
