package com.signomix.common.tsdb;

import com.signomix.common.User;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.UserDaoIface;
import io.agroal.api.AgroalDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

public class UserDao implements UserDaoIface {

    public static final long DEFAULT_ORGANIZATION_ID = 1;

    private static final Logger LOG = Logger.getLogger(UserDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource ds) {
        this.dataSource = ds;
    }

    public void createStructure() throws IotDatabaseException {
        String query;
        query = "create extension if not exists ltree;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
            /*
             * if (!updated) {
             * throw new IotDatabaseException(IotDatabaseException.CONFLICT,
             * "Database structure not updated");
             * }
             */
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

        StringBuilder sb = new StringBuilder();
        // sb.append("create sequence if not exists user_number_seq;");
        // sb.append("create sequence if not exists org_number_seq;");
        sb.append("create table if not exists users (")
                .append("uid varchar PRIMARY KEY,")
                .append("type int,")
                .append("email varchar,")
                .append("role varchar not null default '',")
                .append("secret varchar,")
                .append("password varchar,")
                .append("generalchannel varchar,")
                .append("infochannel varchar,")
                .append("warningchannel varchar,")
                .append("alertchannel varchar,")
                .append("confirmed boolean,")
                .append("unregisterreq boolean,")
                .append("authstatus int,")
                .append("created timestamp not null default current_timestamp,")
                .append("user_number SERIAL,")
                .append("name varchar,")
                .append("surname varchar,")
                .append("services int,")
                .append("phoneprefix varchar,")
                .append("credits bigint,")
                .append("autologin boolean,")
                .append("language varchar,")
                .append("organization bigint default " + DEFAULT_ORGANIZATION_ID + ",") // REMOVED: " references
                                                                                        // organizations(id),"
                .append("path ltree DEFAULT ''::ltree,")
                .append("phone integer);")
                .append("create index if not exists users_user_number_idx on users(user_number);");
        query = sb.toString();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
            /*
             * if (!updated) {
             * throw new IotDatabaseException(IotDatabaseException.CONFLICT,
             * "Database structure not updated");
             * }
             */
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

    }

    @Override
    public void removeNotConfirmed(long days) {
        long seconds = days * 24 * 60 * 60;
        // SELECT uid from users as u where extract(epoch
        // from(current_timestamp-u.created)) > days*24*60*60 and confirmed=false;
        String query = "DELETE FROM users AS u WHERE extract(epoch from(current_timestamp-u.created)) > " + seconds
                + " AND confirmed=false";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            // throw new KeyValueDBException(e.getErrorCode(), e.getMessage());
        }
    }

    @Override
    public List<User> getAll() {
        return new ArrayList<>();
    }

    User buildUser(ResultSet rs) throws SQLException {
        //// uid,type,email,name,surname,role,secret,password,generalchannel,infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,number,services,phoneprefix,credits,autologin
        // "UID","TYPE","EMAIL","ROLE","SECRET","PASSWORD","GENERALCHANNEL","INFOCHANNEL","WARNINGCHANNEL","ALERTCHANNEL","CONFIRMED","UNREGISTERREQ","AUTHSTATUS","CREATED","USER_NUMBER","NAME","SURNAME","SERVICES","PHONEPREFIX","CREDITS","AUTOLOGIN","LANGUAGE","ORGANIZATION"
        User user = new User();
        user.uid = rs.getString("uid");
        user.type = rs.getInt("type");
        user.email = rs.getString("email");
        user.name = rs.getString("name");
        user.surname = rs.getString("surname");
        user.role = rs.getString("role");
        user.confirmString = rs.getString("secret");
        user.password = rs.getString("password");
        user.generalNotificationChannel = rs.getString("generalchannel");
        user.infoNotificationChannel = rs.getString("infochannel");
        user.warningNotificationChannel = rs.getString("warningchannel");
        user.alertNotificationChannel = rs.getString("alertchannel");
        user.confirmed = rs.getBoolean("confirmed");
        user.unregisterRequested = rs.getBoolean("unregisterreq");
        user.authStatus = rs.getInt("authstatus");
        user.createdAt = rs.getTimestamp("created").getTime();
        user.number = rs.getLong("user_number");
        user.services = rs.getInt("services");
        user.phonePrefix = rs.getString("phoneprefix");
        user.credits = rs.getLong("credits");
        user.autologin = rs.getBoolean("autologin");
        user.preferredLanguage = rs.getString("language");
        user.organization = rs.getLong("organization");
        try {
            user.path = rs.getObject("tpath").toString();
        } catch (NullPointerException e) {
            user.path = "";
        }
        try {
            user.tenant = rs.getInt("tenant_id");
        } catch (NullPointerException e) {
            user.tenant = null;
        }
        user.phone = rs.getInt("phone");
        try {
            user.devicesCounter = rs.getInt("devices_counter");
        } catch (Exception e) {
            user.devicesCounter = -1;
        }
        return user;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY users to '/var/lib/postgresql/data/export/users.csv' DELIMITER ';' CSV HEADER;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public User getUser(String uid) throws IotDatabaseException {
        User u = null;
        String query = "SELECT users.*, tenant_users.path AS tpath, tenant_users.tenant_id," +
                "(SELECT count(*) FROM devices WHERE devices.userid=users.uid) AS devices_counter " +
                "FROM users " +
                "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id " +
                "WHERE uid=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, uid);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    u = buildUser(rs);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return u;
    }

    @Override
    public User getUser(long id) throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path AS tpath, tenant_users.tenant_id," +
                "(SELECT count(*) FROM devices WHERE devices.userid=users.uid) AS devices_counter " +
                "FROM users " +
                "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id " +
                "WHERE user_number=?";
        User u = null;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    u = buildUser(rs);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return u;
    }

    @Override
    public User getUser(String login, String password) throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path AS tpath, tenant_users.tenant_id," +
                "(SELECT count(*) FROM devices WHERE devices.userid=users.uid) AS devices_counter " +
                "FROM users " +
                "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id " +
                "WHERE uid=? AND password=?";
        User u = null;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, login);
            pstmt.setString(2, password);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    u = buildUser(rs);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return u;
    }

    @Override
    public List<User> getUsersByRole(String role) throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path AS tpath, tenant_users.tenant_id," +
                "(SELECT count(*) FROM devices WHERE devices.userid=users.uid) AS devices_counter " +
                "FROM users " +
                "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id " +
                "WHERE role LIKE ?";
        ArrayList<User> users = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, "%" + role + "%");
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    users.add(buildUser(rs));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

    @Override
    public void updateUser(User user) throws IotDatabaseException {
        String query = "UPDATE users SET "
                + "type=?,email=?,name=?,surname=?,role=?,secret=?,generalchannel=?,"
                + "infochannel=?,warningchannel=?,alertchannel=?,confirmed=?,unregisterreq=?,authstatus=?,created=?,"
                + "services=?,phoneprefix=?,credits=?,autologin=?,language=?,organization=?, phone=?, password=? "
                + "WHERE uid=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setInt(1, user.type);
            pstmt.setString(2, user.email);
            pstmt.setString(3, user.name);
            pstmt.setString(4, user.surname);
            pstmt.setString(5, user.role != null ? user.role : "");
            pstmt.setString(6, user.confirmString);
            pstmt.setString(7, user.generalNotificationChannel);
            pstmt.setString(8, user.infoNotificationChannel);
            pstmt.setString(9, user.warningNotificationChannel);
            pstmt.setString(10, user.alertNotificationChannel);
            pstmt.setBoolean(11, user.confirmed);
            pstmt.setBoolean(12, user.unregisterRequested);
            pstmt.setInt(13, user.authStatus);
            pstmt.setTimestamp(14, new java.sql.Timestamp(user.createdAt));
            pstmt.setInt(15, user.services);
            pstmt.setString(16, user.phonePrefix);
            pstmt.setLong(17, user.credits);
            pstmt.setBoolean(18, user.autologin);
            pstmt.setString(19, user.preferredLanguage);
            pstmt.setLong(20, user.organization);
            if (user.phone == null) {
                pstmt.setNull(21, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(21, user.phone);
            }
            if (user.password == null || user.password.isEmpty()) {
                pstmt.setNull(22, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(22, user.password);
            }
            pstmt.setString(23, user.uid);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    /**
     * Check if user object has all parameters set or basic parameters or
     * notification parameters
     * only then uses the correct update method
     * 
     * @param user
     * @throws IotDatabaseException
     */
    /*
     * @Override
     * public void updateUser(User user) throws IotDatabaseException {
     * LOG.info("updateUser (new): " + user.uid);
     * if(user.generalNotificationChannel!=null ||
     * user.infoNotificationChannel!=null || user.warningNotificationChannel!=null
     * || user.alertNotificationChannel!=null){
     * updateUserNotifications(user);
     * LOG.info("updateUserNotifications: " + user.uid);
     * }else if(user.email!=null && user.name!=null){
     * updateUserBasicParams(user);
     * LOG.info("updateUserBasicParams: " + user.uid);
     * }
     * }
     * 
     * private void updateUserNotifications(User user) throws IotDatabaseException {
     * LOG.info("generalNotificationChannel: " + user.generalNotificationChannel);
     * String query = "UPDATE users SET "
     * + "generalchannel=?,infochannel=?,warningchannel=?,alertchannel=? "
     * + "WHERE uid=?";
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt =
     * conn.prepareStatement(query);) {
     * pstmt.setString(1,
     * user.generalNotificationChannel==null?"":user.generalNotificationChannel);
     * pstmt.setString(2,
     * user.infoNotificationChannel==null?"":user.infoNotificationChannel);
     * pstmt.setString(3,
     * user.warningNotificationChannel==null?"":user.warningNotificationChannel);
     * pstmt.setString(4,
     * user.alertNotificationChannel==null?"":user.alertNotificationChannel);
     * pstmt.setString(5, user.uid);
     * pstmt.executeUpdate();
     * } catch (SQLException e) {
     * e.printStackTrace();
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
     * e.getMessage());
     * } catch (Exception e) {
     * e.printStackTrace();
     * throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
     * }
     * }
     */

    /**
     * Update all user parameters except notifications
     * 
     * @param user
     */
    /*
     * private void updateUserBasicParams(User user) throws IotDatabaseException{
     * String query = "UPDATE users SET "
     * + "type=?,email=?,name=?,surname=?,role=?,secret=?,"
     * + "confirmed=?,unregisterreq=?,authstatus=?,created=?,"
     * +
     * "services=?,phoneprefix=?,credits=?,autologin=?,language=?,organization=?, phone=? "
     * + "WHERE uid=?";
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt =
     * conn.prepareStatement(query);) {
     * pstmt.setInt(1, user.type);
     * pstmt.setString(2, user.email);
     * pstmt.setString(3, user.name);
     * pstmt.setString(4, user.surname);
     * pstmt.setString(5, user.role!=null?user.role:"");
     * pstmt.setString(6, user.confirmString);
     * pstmt.setBoolean(7, user.confirmed);
     * pstmt.setBoolean(8, user.unregisterRequested);
     * pstmt.setInt(9, user.authStatus);
     * pstmt.setTimestamp(10, new java.sql.Timestamp(user.createdAt));
     * pstmt.setInt(11, user.services);
     * if(user.phonePrefix==null){
     * user.phonePrefix="";
     * }else{
     * pstmt.setString(12, user.phonePrefix);
     * }
     * if(user.credits==null){
     * user.credits=0L;
     * }else{
     * pstmt.setLong(13, user.credits);
     * }
     * pstmt.setBoolean(14, user.autologin);
     * pstmt.setString(15, user.preferredLanguage);
     * pstmt.setLong(16, user.organization);
     * if (user.phone == null) {
     * pstmt.setNull(17, java.sql.Types.INTEGER);
     * } else {
     * pstmt.setInt(17, user.phone);
     * }
     * pstmt.setString(18, user.uid);
     * pstmt.executeUpdate();
     * } catch (SQLException e) {
     * e.printStackTrace();
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
     * e.getMessage());
     * }
     * }
     */

    /*    
    */
    @Override
    public List<User> getOrganizationUsers(long organizationId, Integer limit, Integer offset, String searchField,
            String searchValue)
            throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path AS tpath, tenant_users.tenant_id," +
                "(SELECT count(*) FROM devices WHERE devices.userid=users.uid) AS devices_counter " +
                "FROM users " +
                "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id " +
                "WHERE organization=? AND tenant_users.user_id IS NULL ";

        String wherePart;
        int numberOfSearchParams = 0;
        switch (searchField) {
            case "email":
                wherePart = " AND email LIKE ?";
                numberOfSearchParams = 1;
                break;
            case "name":
                wherePart = " AND name LIKE ? OR surname LIKE ?";
                numberOfSearchParams = 2;
                break;
            case "login":
                wherePart = " AND uid LIKE ?";
                numberOfSearchParams = 1;
                break;
            /*
             * case "path":
             * wherePart = " AND path LIKE ?";
             * numberOfSearchParams = 1;
             * break;
             */
            case "role":
                wherePart = " AND role LIKE ?";
                numberOfSearchParams = 1;
                break;
            default:
                wherePart = "";
        }
        query += wherePart;
        if (limit != null) {
            query += " LIMIT " + limit;
        }
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        LOG.info("getOrganizationUsers: " + query);
        ArrayList<User> users = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, organizationId);
            if (!wherePart.isEmpty()) {
                pstmt.setString(2, searchValue);
                if (numberOfSearchParams > 1) {
                    pstmt.setString(3, searchValue);
                }
            }
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    users.add(buildUser(rs));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

    @Override
    public List<User> getUsers(Integer limit, Integer offset, String searchField, String searchValue)
            throws IotDatabaseException {
        // String query = "SELECT * from users";

        String wherePart;
        int numberOfSearchParams = 0;
        if (searchField == null) {
            searchField = "";
        }
        switch (searchField) {
            case "email":
                wherePart = " WHERE email LIKE ?";
                numberOfSearchParams = 1;
                break;
            case "name":
                wherePart = " WHERE name LIKE ? OR surname LIKE ?";
                numberOfSearchParams = 2;
                break;
            case "login":
                wherePart = " WHERE uid LIKE ?";
                numberOfSearchParams = 1;
                break;
            /*
             * case "path":
             * wherePart = " WHERE path LIKE ?";
             * numberOfSearchParams = 1;
             * break;
             */
            case "role":
                wherePart = " WHERE role LIKE ?";
                numberOfSearchParams = 1;
                break;
            default:
                wherePart = "";
        }
        String query = "SELECT users.*, tenant_users.path AS tpath, tenant_users.tenant_id," +
                "(SELECT count(*) FROM devices WHERE devices.userid=users.uid) AS devices_counter " +
                "FROM users " +
                "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id ";
        query += wherePart;
        if (limit != null) {
            query += " LIMIT " + limit;
        }
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        ArrayList<User> users = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            if (!wherePart.isEmpty()) {
                pstmt.setString(1, "%" + searchValue + "%");
                if (numberOfSearchParams > 1) {
                    pstmt.setString(2, "%" + searchValue + "%");
                }
            }
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    users.add(buildUser(rs));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

    @Override
    public Integer addUser(User user) throws IotDatabaseException {
        String query = "INSERT INTO users "
                + "(uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "services,phoneprefix,credits,autologin,language,organization, phone) "
                + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);) {
            pstmt.setString(1, user.uid);
            pstmt.setInt(2, user.type);
            pstmt.setString(3, user.email);
            pstmt.setString(4, user.name);
            pstmt.setString(5, user.surname);
            pstmt.setString(6, user.role != null ? user.role : "");
            pstmt.setString(7, user.confirmString);
            pstmt.setString(8, user.password);
            pstmt.setString(9, user.generalNotificationChannel);
            pstmt.setString(10, user.infoNotificationChannel);
            pstmt.setString(11, user.warningNotificationChannel);
            pstmt.setString(12, user.alertNotificationChannel);
            pstmt.setBoolean(13, user.confirmed);
            pstmt.setBoolean(14, user.unregisterRequested);
            pstmt.setInt(15, user.authStatus);
            if (user.createdAt == null || user.createdAt == 0) {
                user.createdAt = System.currentTimeMillis();
            }
            pstmt.setTimestamp(16, new java.sql.Timestamp(user.createdAt));
            pstmt.setInt(17, user.services == null ? 0 : user.services);
            pstmt.setString(18, user.phonePrefix);
            pstmt.setLong(19, user.credits == null ? 0L : user.credits);
            pstmt.setBoolean(20, user.autologin);
            pstmt.setString(21, user.preferredLanguage);
            pstmt.setLong(22, user.organization);
            if (user.phone == null) {
                pstmt.setNull(23, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(23, user.phone);
            }
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        // get user number
        int userNumber = -1;
        query = "SELECT user_number FROM users WHERE uid=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, user.uid);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    userNumber = rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        if (userNumber < 0) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, "User number not found");
        }
        return userNumber;
    }

    @Override
    public void deleteUser(long id) throws IotDatabaseException {
        String query = "DELETE FROM users WHERE user_number=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void modifyUserPassword(long id, String password) throws IotDatabaseException {
        String query = "UPDATE users SET password=? WHERE user_number=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, password);
            pstmt.setLong(2, id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public List<User> getTenantUsers(long tenantId, Integer limit, Integer offset, String searchField,
            String searchValue) throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path AS tpath, tenant_users.tenant_id," +
                "(SELECT count(*) FROM devices WHERE devices.userid=users.uid) AS devices_counter " +
                "FROM users " +
                "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id " +
                "WHERE tenant_users.tenant_id=?";
        String wherePart;
        int numberOfSearchParams = 0;
        switch (searchField) {
            case "email":
                wherePart = " AND email LIKE ?";
                numberOfSearchParams = 1;
                break;
            case "name":
                wherePart = " AND name LIKE ? OR surname LIKE ?";
                numberOfSearchParams = 2;
                break;
            case "login":
                wherePart = " AND uid LIKE ?";
                numberOfSearchParams = 1;
                break;
            /*
             * case "path":
             * wherePart = " AND path LIKE ?";
             * numberOfSearchParams = 1;
             * break;
             */
            case "role":
                wherePart = " AND role LIKE ?";
                numberOfSearchParams = 1;
                break;
            default:
                wherePart = "";
        }
        query += wherePart;
        if (limit != null) {
            query += " LIMIT " + limit;
        }
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        ArrayList<User> users = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, tenantId);
            if (!wherePart.isEmpty()) {
                pstmt.setString(2, searchValue);
                if (numberOfSearchParams > 1) {
                    pstmt.setString(3, searchValue);
                }
            }
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    users.add(buildUser(rs));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

    @Override
    public void updateTenantUser(User user) throws IotDatabaseException {
        String query = "UPDATE tenant_users SET path=?, updated_at=? WHERE user_id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setObject(1, user.path, java.sql.Types.OTHER);
            pstmt.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
            pstmt.setLong(3, user.number);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void addTenantUser(Long organizationId, Integer tenantId, Long userNumber, String path)
            throws IotDatabaseException {
        String query = "INSERT INTO tenant_users(organization_id, tenant_id, user_id, path) VALUES(?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setInt(2, tenantId);
            pst.setLong(3, userNumber);
            pst.setObject(4, path, java.sql.Types.OTHER);
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

}
