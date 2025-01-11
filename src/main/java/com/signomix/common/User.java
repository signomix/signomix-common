package com.signomix.common;

import org.jboss.logging.Logger;

//import javax.enterprise.context.Dependent;

//@Dependent
public class User {

    public static Logger logger=Logger.getLogger(User.class);

    public static final int USER = 0; // default type, standard user
    public static final int OWNER = 1; // owner, service admin
    public static final int APPLICATION = 2; // application
    public static final int DEMO = 3;
    public static final int FREE = 4; // registered, free account
    public static final int PRIMARY = 5; // primary account
    public static final int READONLY = 6;
    public static final int EXTENDED = 7; // students, scientists, nonprofits
    public static final int SUPERUSER = 8; // organization admin
    public static final int MANAGING_ADMIN = 8; // organization admin
    public static final int ORGANIZATION_ADMIN = 8; // organization admin
    public static final int ADMIN = 9; // tenant admin
    public static final int TENANT_ADMIN = 9; // tenant admin
    public static final int ANONYMOUS = 10;
    public static final int SUBSCRIBER = 100;
    public static final int ANY = 1000;

    public static final int[] PAID_TYPES = {USER, OWNER, PRIMARY, EXTENDED, SUPERUSER, ADMIN, MANAGING_ADMIN, ORGANIZATION_ADMIN, TENANT_ADMIN};

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
    public Long createdAt;
    public Long number;
    public Integer services;
    public String phonePrefix;
    public Long credits;
    public Boolean autologin;
    public String preferredLanguage;
    public Long organization;
    public String sessionToken;
    public String organizationCode;
    public String path="";
    public Integer phone;
    public Integer tenant;
    public String pathRoot="";
    public Integer devicesCounter = null;

    /**
     * Clones given user
     * @param cloned
     * @return cloned user
     */
    public static User clone(User cloned){
        User result=new User();

        if(cloned.alertNotificationChannel!=null) result.alertNotificationChannel=cloned.alertNotificationChannel;
        if(cloned.authStatus!=null) result.authStatus=cloned.authStatus;
        if(cloned.autologin!=null) result.autologin=cloned.autologin;
        if(cloned.confirmString!=null) result.confirmString=cloned.confirmString;
        if(cloned.confirmed!=null) result.confirmed=cloned.confirmed;
        if(cloned.createdAt!=null) result.createdAt=cloned.createdAt;
        if(cloned.credits!=null) result.credits=cloned.credits;
        if(cloned.email!=null) result.email=cloned.email;
        if(cloned.generalNotificationChannel!=null) result.generalNotificationChannel=cloned.generalNotificationChannel;
        if(cloned.infoNotificationChannel!=null) result.infoNotificationChannel=cloned.infoNotificationChannel;
        if(cloned.name!=null) result.name=cloned.name;
        if(cloned.number!=null) result.number=cloned.number;
        if(cloned.organization!=null) result.organization=cloned.organization;
        if(cloned.organizationCode!=null) result.organizationCode=cloned.organizationCode;
        if(cloned.password!=null) result.password=cloned.password;
        if(cloned.path!=null) result.path=cloned.path;
        if(cloned.tenant!=null) result.tenant=cloned.tenant;
        if(cloned.phonePrefix!=null) result.phonePrefix=cloned.phonePrefix;
        if(cloned.phone!=null) result.phone=cloned.phone;
        if(cloned.preferredLanguage!=null) result.preferredLanguage=cloned.preferredLanguage;
        if(cloned.role!=null) result.role=cloned.role;
        if(cloned.services!=null) result.services=cloned.services;
        if(cloned.sessionToken!=null) result.sessionToken=cloned.sessionToken;
        if(cloned.surname!=null) result.surname=cloned.surname;
        if(cloned.type!=null) result.type=cloned.type;
        if(cloned.uid!=null) result.uid=cloned.uid;
        if(cloned.unregisterRequested!=null) result.unregisterRequested=cloned.unregisterRequested;
        if(cloned.warningNotificationChannel!=null) result.warningNotificationChannel=cloned.warningNotificationChannel;
        if(cloned.devicesCounter!=null) result.devicesCounter=cloned.devicesCounter;
        
        // fix nulls
        if (cloned.services == null) {
            cloned.services = 0;
        }
        if (cloned.credits == null) {
            cloned.credits = 0L;
        }
        if (cloned.autologin == null) {
            cloned.autologin = false;
        }
        return result;
    }
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

    public String getPathRoot() {
        if (path == null) {
            return "";
        }
        String[] parts = path.split("\\.");
        if (parts.length > 0) {
            return parts[0];
        }
        return "";
    }

}
