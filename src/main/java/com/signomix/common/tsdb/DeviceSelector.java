package com.signomix.common.tsdb;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import com.signomix.common.User;

public class DeviceSelector {
    public static final Logger logger = Logger.getLogger(DeviceSelector.class);

    Long defaultOrganizationId = ConfigProvider.getConfig().getValue("signomix.default.organization.id", Long.class);

    // public static final int defaultOrganizationId = -1;
    public String userSql;
    public String writable;
    public int numberOfWritableParams;
    public int numberOfUserParams;
    public String query;
    public String limitSql;
    public String offsetSql;
    public String orderSql = "";
    public String searchCondition = "";
    public int numberOfSearchParams;

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
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp, d.defaultdashboard, a.configuration, ")
                .append(this.writable)
                .append(" FROM devices AS d ")
                .append(" LEFT JOIN applications AS a ON d.organizationapp=a.id WHERE d.active = true  AND d.tinterval>0 ");
        query = sb.toString();
    }

    public DeviceSelector(boolean inactive) {

        /*
         * SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code,
         * d.decoder,d.devicekey, d.description,
         * d.tinterval, d.template, d.pattern, d.commandscript, d.appid,d.groups,
         * d.devid, d.appeui, d.active, d.project,
         * d.latitude, d.longitude, d.altitude, d.retention,
         * d.administrators,d.framecheck, d.configuration, d.organization,
         * d.organizationapp, a.configuration, false as writable
         * FROM devices AS d
         * LEFT JOIN applications AS a
         * ON d.organizationapp=a.id
         * WHERE EXISTS
         * (
         * SELECT DISTINCT ON (ds.eui) ds.eui, ds.ts FROM devicestatus AS ds
         * WHERE ds.eui=d.eui AND ds.alert<2 AND ds.tinterval>0 AND
         * ds.ts < (CURRENT_TIMESTAMP - ds.tinterval * INTERVAL '1 millisecond')
         * )
         * ORDER BY d.eui DESC LIMIT 100 OFFSET 0
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
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp, d.defaultdashboard, a.configuration, ")
                .append("false as writable")
                .append(" FROM devices AS d ")
                .append(" LEFT JOIN applications AS a ON d.organizationapp=a.id ")
                .append(" AND EXISTS ")
                .append(q0)
                .append("  ORDER BY d.eui");
        query = sb.toString();
    }

    public DeviceSelector(User user, boolean withShared, boolean withStatus, boolean single, Integer limit,
            Integer offset, String searchString) {
        numberOfWritableParams = 0;
        numberOfUserParams = 0;
        numberOfSearchParams = 0;
        limitSql = "";
        offsetSql = "";
        orderSql = " ORDER BY d.name ";
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
                // } else if (user.organization != defaultOrganizationId && user.type ==
                // User.ADMIN) {
                // this.userSql = "";
                // this.writable = "true as writable";
                // } else if (user.organization != defaultOrganizationId && user.type !=
                // User.ADMIN) {
                // // organization admin
                // this.userSql = "";
                // this.writable = "false as writable";
            } else {
                // default organization
                this.userSql = " (d.userid=? OR d.team like ? OR d.administrators like ?) ";
                numberOfUserParams = 3;
                this.writable = "(d.userid=? OR d.administrators like ?) AS writable";
                this.numberOfWritableParams = 2;
            }
            if (null != limit) {
                this.limitSql = " LIMIT " + limit;
            }
            if (null != offset) {
                this.offsetSql = " OFFSET " + offset;
            }
            if (searchString != null) {
                String[] searchParts = searchString.split(":");
                if (searchParts.length == 2) {
                    if (searchParts[0].equals("eui")) {
                        searchCondition = " LOWER(d.eui) LIKE LOWER(?) ";
                        numberOfSearchParams = 1;
                    } else if (searchParts[0].equals("name")) {
                        searchCondition = " LOWER(d.name) LIKE LOWER(?) ";
                        numberOfSearchParams = 1;
                    }
                } else if (searchParts.length == 3 && searchParts[0].equals("tag")) {
                    searchCondition = " LOWER(d.eui) IN (SELECT LOWER(eui) FROM device_tags WHERE LOWER(tag_name) LIKE LOWER(?) AND LOWER(tag_value) LIKE LOWER(?))";
                    numberOfSearchParams = 2;
                }
            }

        }

        StringBuffer sb = new StringBuffer()
                .append("SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,")
                .append("d.devicekey, d.description, d.tinterval, d.template, d.pattern, d.commandscript, d.appid,")
                .append("d.groups, d.devid, d.appeui, d.active, d.project, d.latitude, d.longitude, d.altitude, d.retention, d.administrators,")
                .append("d.framecheck, d.configuration, d.organization, d.organizationapp, d.defaultdashboard, a.configuration, ")
                .append(this.writable)
                .append(" FROM devices AS d ")
                .append(" LEFT JOIN applications AS a ON d.organizationapp=a.id ");
        if (single || this.userSql.length() > 0 || this.searchCondition.length() > 0) {
            sb.append(" WHERE  ");
            if (single) {
                sb = sb.append(" d.eui=? ");
            } else if (searchString != null && !searchString.isEmpty()) {
                sb = sb.append(searchCondition);
            }
            if ((single || searchCondition.length() > 0) && this.userSql.length() > 0) {
                sb = sb.append(" AND ");
            }
            if (this.userSql.length() > 0) {
                sb = sb.append(this.userSql);
            }
        }

        sb = sb.append(this.orderSql)
                .append(this.limitSql)
                .append(this.offsetSql);
        query = sb.toString();
        logger.info("query:" + query);
    }

    public DeviceSelector(String deviceEui, boolean withStatus) {
        String writable = "false as writable ";
        query = "SELECT d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder,"
                + "d.devicekey, d.description, d.tinterval, d.template, d.pattern, d.commandscript, d.appid,"
                + "d.groups, d.devid, d.appeui, d.active, d.project, d.latitude, d.longitude, d.altitude, d.retention, d.administrators,"
                + "d.framecheck, d.configuration, d.organization, d.organizationapp, d.defaultdashboard, a.configuration, "
                + writable
                + "FROM devices AS d "
                + "LEFT JOIN applications AS a ON d.organizationapp=a.id "
                + "WHERE d.eui=? ";
    }

}
