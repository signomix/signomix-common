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
    public String limitSql;
    public String offsetSql;

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

        /*
SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,d.devicekey, d.description, 
d.tinterval, d.template, d.pattern, d.commandscript, d.appid,d.groups, d.devid, d.appeui, d.active, d.project, 
d.latitude, d.longitude, d.altitude, d.retention, d.administrators,d.framecheck, d.configuration, d.organization, 
d.organizationapp, a.configuration, false as writable 
FROM devices AS d  
LEFT JOIN applications AS a 
ON d.organizationapp=a.id 
WHERE EXISTS 
( 
  SELECT DISTINCT ON (ds.eui) ds.eui, ds.ts FROM devicestatus AS ds 
  WHERE ds.eui=d.eui AND ds.alert<2 AND ds.tinterval>0 AND 
  ds.ts < (CURRENT_TIMESTAMP - ds.tinterval * INTERVAL '1 millisecond')
)
ORDER BY d.eui DESC LIMIT 100 OFFSET 0
         */
        String q0 = "(SELECT DISTINCT ON (ds.eui) "
                + "ds.eui, ds.ts "
                + "FROM devicestatus AS ds "
                + "WHERE ds.eui=d.eui AND ds.alert<2 AND ds.tinterval>0 AND ds.ts < (CURRENT_TIMESTAMP - ds.tinterval * INTERVAL '1 millisecond') "
                + ")";
        StringBuffer sb = new StringBuffer()
                .append("SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,")
                .append("d.devicekey, d.description, d.tinterval, d.template, d.pattern, d.commandscript, d.appid,")
                .append("d.groups, d.devid, d.appeui, d.active, d.project, d.latitude, d.longitude, d.altitude, d.retention, d.administrators,")
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp, a.configuration, ")
                .append("false as writable")
                .append(" FROM devices AS d ")
                .append(" LEFT JOIN applications AS a ON d.organizationapp=a.id ")
                .append(" AND EXISTS ")
                .append(q0)
                .append("  ORDER BY d.eui )");
        query = sb.toString();
    }

    public DeviceSelector(User user, boolean withShared, boolean withStatus, boolean single, Integer limit, Integer offset) {
        numberOfWritableParams = 0;
        numberOfUserParams = 0;
        limitSql="";
        offsetSql="";
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
            //} else if (user.organization != defaultOrganizationId && user.type == User.ADMIN) {
            //    this.userSql = "";
            //    this.writable = "true as writable";
            //} else if (user.organization != defaultOrganizationId && user.type != User.ADMIN) {
            //    // organization admin
            //    this.userSql = "";
            //    this.writable = "false as writable";
            } else {
                // default organization
                this.userSql = "WHERE (d.userid=? OR d.team like ? OR d.administrators like ?) ";
                numberOfUserParams = 3;
                this.writable = "(d.userid=? OR d.administrators like ?) AS writable";
                this.numberOfWritableParams = 2;
            }
            if(null!=limit){
                this.limitSql=" LIMIT "+limit;
            }
            if(null!=offset){
                this.offsetSql=" OFFSET "+offset;
            }
        }

        StringBuffer sb = new StringBuffer()
                .append("SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,")
                .append("d.devicekey, d.description, d.tinterval, d.template, d.pattern, d.commandscript, d.appid,")
                .append("d.groups, d.devid, d.appeui, d.active, d.project, d.latitude, d.longitude, d.altitude, d.retention, d.administrators,")
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp, a.configuration, ")
                .append(this.writable)
                .append(" FROM devices AS d ")
                .append(" LEFT JOIN applications AS a ON d.organizationapp=a.id ")
                .append( (single ? "AND d.eui=? " : ""))
                .append(this.userSql)
                .append(this.limitSql)
                .append(this.offsetSql);
        query = sb.toString();
    }

}
