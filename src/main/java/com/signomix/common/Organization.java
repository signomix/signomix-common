package com.signomix.common;

public class Organization {
    public Long id;
    public String code;
    public String name;
    public String description;
    public Integer numberOfTenants;

    public Organization(Long id, String code, String name, String description, Integer numberOfTenants) {
        this.id = id;
        this.name = name;
        this.code = code;
        this.description = description;
        this.numberOfTenants = numberOfTenants;
    }
}
