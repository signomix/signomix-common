package com.signomix.common;

//import javax.enterprise.context.Dependent;

//@Dependent
public class User {

    public static final int USER = 0; // default type, standard user
    public static final int OWNER = 1; // owner, service admin
    public static final int APPLICATION = 2; // application
    public static final int DEMO = 3;
    public static final int FREE = 4; // registered, free account
    public static final int PRIMARY = 5; // primary account
    public static final int READONLY = 6;
    public static final int EXTENDED = 7; // students, scientists, nonprofits
    public static final int SUPERUSER = 8;
    public static final int ADMIN = 9; // organization admin
    public static final int ANONYMOUS = 10;
    public static final int SUBSCRIBER = 100;
    public static final int ANY = 1000;

    public static final int IS_REGISTERING = 0;
    public static final int IS_ACTIVE = 1;
    public static final int IS_UNREGISTERING = 2;
    public static final int IS_LOCKED = 3;
    public static final int IS_CREATED = 10;

    public static final int SERVICE_SMS = 0b00000001;
    public static final int SERVICE_SUPPORT = 0b00000010; // not used
    public static final int SERVICE_3 = 0b00000100; // not used
    public static final int SERVICE_4 = 0b00001000; // not used
    public static final int SERVICE_5 = 0b00010000; // not used
    public static final int SERVICE_6 = 0b00100000; // not used
    public static final int SERVICE_7 = 0b01000000; // not used
    public static final int SERVICE_8 = 0b10000000; // not used

    public Integer type;
    public String uid;
    public String email;
    public String name;
    public String surname;
    public String role;
    public Boolean confirmed;
    public Boolean unregisterRequested;
    public String confirmString;
    public String password;
    public String generalNotificationChannel = "";
    public String infoNotificationChannel = "";
    public String warningNotificationChannel = "";
    public String alertNotificationChannel = "";
    public Integer authStatus;
    public long createdAt;
    public long number;
    public int services;
    public String phonePrefix;
    public long credits;
    public boolean autologin;
    public String preferredLanguage;
    public long organization;
    public String sessionToken;
    public String organizationCode;

    public User() {
    }

    public String[] getChannelConfig(String eventTypeName) {
        String channel = "";
        switch (eventTypeName.toUpperCase()) {
            case "GENERAL":
            case "DEVICE_LOST":
                channel = generalNotificationChannel;
                break;
            case "INFO":
                channel = infoNotificationChannel;
                break;
            case "WARNING":
                channel = warningNotificationChannel;
                break;
            case "ALERT":
                channel = alertNotificationChannel;
                break;
        }
        if (channel == null) {
            channel = "";
        }
        return channel.split(":");
    }

    public boolean checkPassword(String passToCheck) {
        return password != null && password.equals(HashMaker.md5Java(passToCheck));
    }

    public void addService(int newService) {
        services = services | newService;
    }

    public void removeService(int newService) {
        services = services ^ newService;
    }

    public boolean hasService(int newService) {
        return (services & newService) == newService;
    }

}
