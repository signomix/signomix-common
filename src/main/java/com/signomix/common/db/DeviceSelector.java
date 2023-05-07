package com.signomix.common.db;

import com.signomix.common.User;

public class DeviceSelector {
    public static final int DEFAULT_ORGANIZATION_ID = -1;
    public String userSql;
    public String writable;
    public int numberOfWritableParams;
    public int numberOfUserParams;
    public String query;

    public DeviceSelector(User user, boolean withShared, boolean withStatus, boolean single) {
        numberOfWritableParams = 0;
        numberOfUserParams = 0;
        if (user.type == User.OWNER) {
            // system admin
            this.userSql = "";
            this.writable = "true";
        } else if (user.organization != DEFAULT_ORGANIZATION_ID && user.type == User.ADMIN) {
            this.userSql = "";
            this.writable = "true";
        } else if (user.organization != DEFAULT_ORGANIZATION_ID && user.type != User.ADMIN) {
            // organization admin
            this.userSql = "";
            this.writable = "false";
        } else {
            // default organization
            this.userSql = " " + (single ? "AND" : "") + " (d.userid=? OR d.team like ? OR d.administrators like ?) ";
            numberOfUserParams = 3;
            this.writable = "(d.userid=? OR d.administrators like ?)";
            this.numberOfWritableParams = 2;
        }

        StringBuffer sb = new StringBuffer()
                .append("SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,")
                .append("d.devicekey, d.description, d.tinterval, d.template, d.pattern, d.commandscript, d.appid,")
                .append("d.groups, d.appeui, d.active, d.project, d.latitude, d.longitude, d.altitude, d.retention, d.administrators,")
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp,")
                .append(this.writable)
                .append(" FROM devices d " + (single ? "WHERE d.eui=? " : ""))
                .append(this.userSql);
        query = sb.toString();
    }

}
