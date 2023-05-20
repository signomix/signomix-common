package com.signomix.common.db;

import org.eclipse.microprofile.config.ConfigProvider;

import com.signomix.common.User;

public class DeviceSelector {

    Long defaultOrganizationId = ConfigProvider.getConfig().getValue("signomix.default.organization.id", Long.class);

    // public static final int defaultOrganizationId = -1;
    public String userSql;
    public String writable;
    public int numberOfWritableParams;
    public int numberOfUserParams;
    public String query;

    /**
     * Constructor for DeviceSelector.
     * 
     * Selects all active devices with tinterval>0
     */
    public DeviceSelector() {
        StringBuffer sb = new StringBuffer()
                .append("SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,")
                .append("d.devicekey, d.description, d.tinterval, d.template, d.pattern, d.commandscript, d.appid,")
                .append("d.groups, d.devid, d.appeui, d.active, d.project, d.latitude, d.longitude, d.altitude, d.retention, d.administrators,")
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp, a.configuration, ")
                .append(this.writable)
                .append(" FROM devices AS d ")
                .append(" LEFT JOIN applications AS a WHERE d.organizationapp=a.id AND d.active = true  AND d.tinterval>0 ");
        query = sb.toString();
    }

    public DeviceSelector(boolean inactive) {
        String q0 = "(SELECT DISTINCT ON (ds.eui) "
                + "ds.eui, ds.ts "
                + "FROM devicestatus AS ds "
                + "WHERE ds.eui=d.eui AND ds.alert<2 AND ds.tinterval>0 AND ds.ts < TIMESTAMPADD('MILLISECOND', -1*ds.tinterval, CURRENT_TIMESTAMP) "
                + "ORDER BY ds.eui, ds.ts DESC)";
        StringBuffer sb = new StringBuffer()
                .append("SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,")
                .append("d.devicekey, d.description, d.tinterval, d.template, d.pattern, d.commandscript, d.appid,")
                .append("d.groups, d.devid, d.appeui, d.active, d.project, d.latitude, d.longitude, d.altitude, d.retention, d.administrators,")
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp, a.configuration, ")
                .append("false as writable")
                .append(" FROM devices AS d ")
                .append(" LEFT JOIN applications AS a WHERE d.organizationapp=a.id ")
                .append(" AND EXISTS ")
                .append(q0);
        query = sb.toString();
    }

    public DeviceSelector(User user, boolean withShared, boolean withStatus, boolean single) {
        numberOfWritableParams = 0;
        numberOfUserParams = 0;
        if (user == null) {
            // anonymous
            this.userSql = "";
            numberOfUserParams = 0;
            this.writable = "true as writable";
        } else {
            if (user.type == User.OWNER) {
                // system admin
                this.userSql = "";
                this.writable = "true as writable";
            } else if (user.organization != defaultOrganizationId && user.type == User.ADMIN) {
                this.userSql = "";
                this.writable = "true as writable";
            } else if (user.organization != defaultOrganizationId && user.type != User.ADMIN) {
                // organization admin
                this.userSql = "";
                this.writable = "false as writable";
            } else {
                // default organization
                this.userSql = "AND (d.userid=? OR d.team like ? OR d.administrators like ?) ";
                numberOfUserParams = 3;
                this.writable = "(d.userid=? OR d.administrators like ?) AS writable";
                this.numberOfWritableParams = 2;
            }
        }

        StringBuffer sb = new StringBuffer()
                .append("SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,")
                .append("d.devicekey, d.description, d.tinterval, d.template, d.pattern, d.commandscript, d.appid,")
                .append("d.groups, d.devid, d.appeui, d.active, d.project, d.latitude, d.longitude, d.altitude, d.retention, d.administrators,")
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp, a.configuration, ")
                .append(this.writable)
                .append(" FROM devices AS d ")
                .append(" LEFT JOIN applications AS a WHERE d.organizationapp=a.id ")
                .append( (single ? "AND d.eui=? " : ""))
                .append(this.userSql);
        query = sb.toString();
    }

}
