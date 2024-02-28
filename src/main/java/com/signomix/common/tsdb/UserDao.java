package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.HashMaker;
import com.signomix.common.User;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.UserDaoIface;

import io.agroal.api.AgroalDataSource;

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
                .append("uid varchar primary key,")
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
                .append("organization bigint default " + DEFAULT_ORGANIZATION_ID + " references organizations(id),")
                .append("path ltree DEFAULT ''::ltree,")
                .append("phone integer);");
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

        User user = new User();
        user.uid = "admin";
        user.type = User.OWNER;
        user.email = "";
        user.name = "admin";
        user.surname = "admin";
        user.role = "";
        user.confirmString = "";
        user.password = HashMaker.md5Java("test123");
        user.generalNotificationChannel = "";
        user.infoNotificationChannel = "";
        user.warningNotificationChannel = "";
        user.alertNotificationChannel = "";
        user.confirmed = true;
        user.unregisterRequested = false;
        user.authStatus = 1;
        user.createdAt = System.currentTimeMillis();
        user.number = 0L;
        user.services = 0;
        user.phonePrefix = "";
        user.credits = 0L;
        user.autologin = false;
        user.preferredLanguage = "en";
        user.organization = DEFAULT_ORGANIZATION_ID;
        user.path = "";
        try {
            addUser(user);
        } catch (IotDatabaseException e) {
            LOG.warn("Error inserting default admin user", e);
        }
        user = new User();
        user.uid = "tester1";
        user.type = User.USER;
        user.email = "";
        user.name = "tester";
        user.surname = "tester";
        user.role = "";
        user.confirmString = "";
        user.password = HashMaker.md5Java("signomix");
        user.generalNotificationChannel = "";
        user.infoNotificationChannel = "";
        user.warningNotificationChannel = "";
        user.alertNotificationChannel = "";
        user.confirmed = true;
        user.unregisterRequested = false;
        user.authStatus = 1;
        user.createdAt = System.currentTimeMillis();
        user.number = null;
        user.services = 0;
        user.phonePrefix = "";
        user.credits = 0L;
        user.autologin = false;
        user.preferredLanguage = "en";
        user.organization = DEFAULT_ORGANIZATION_ID;
        user.path = "";
        try {
            addUser(user);
        } catch (IotDatabaseException e) {
            LOG.warn("Error inserting default admin user", e);
        }
        user = new User();
        user.uid = "public";
        user.type = User.READONLY;
        user.email = "";
        user.name = "Public";
        user.surname = "User";
        user.role = "";
        user.confirmString = "";
        user.password = HashMaker.md5Java("public");
        user.generalNotificationChannel = "";
        user.infoNotificationChannel = "";
        user.warningNotificationChannel = "";
        user.alertNotificationChannel = "";
        user.confirmed = true;
        user.unregisterRequested = false;
        user.authStatus = 1;
        user.createdAt = System.currentTimeMillis();
        user.number = null;
        user.services = 0;
        user.phonePrefix = "";
        user.credits = 0L;
        user.autologin = false;
        user.preferredLanguage = "en";
        user.organization = DEFAULT_ORGANIZATION_ID;
        user.path = "";
        try {
            addUser(user);
        } catch (IotDatabaseException e) {
            LOG.warn("Error inserting default admin user", e);
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
        try{
            user.path = rs.getObject("path").toString();
        }catch(NullPointerException e){
            user.path="";
        }
        try{
            user.tenant=rs.getInt("tenant_id");
        }catch(NullPointerException e){
            user.tenant=null;
        }
        user.phone = rs.getInt("phone");
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
        String query = "SELECT users.*, tenant_users.path, tenant_users.tenant_id FROM users "
                + "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id "
                + "WHERE uid=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, uid);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return buildUser(rs);
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return null;
    }

    @Override
    public User getUser(long id) throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path, tenant_users.tenant_id FROM users "
                + "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id "
                + "WHERE user_number=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return buildUser(rs);
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return null;
    }

    @Override
    public User getUser(String login, String password) throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path, tenant_users.tenant_id FROM users "
                + "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id "
                + "WHERE uid=? AND password=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, login);
            pstmt.setString(2, password);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return buildUser(rs);
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return null;
    }

    @Override
    public List<User> getUsersByRole(String role) throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path, tenant_users.tenant_id FROM users "
                + "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id"
                + "WHERE role LIKE ?";
        ArrayList<User> users = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, "%," + role + ",%");
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                users.add(buildUser(rs));
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return null;
    }

    @Override
    public void updateUser(User user) throws IotDatabaseException {
        String query = "UPDATE users SET "
                + "type=?,email=?,name=?,surname=?,role=?,secret=?,generalchannel=?,"
                + "infochannel=?,warningchannel=?,alertchannel=?,confirmed=?,unregisterreq=?,authstatus=?,created=?,"
                + "services=?,phoneprefix=?,credits=?,autologin=?,language=?,organization=?, phone=? "
                + "WHERE uid=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setInt(1, user.type);
            pstmt.setString(2, user.email);
            pstmt.setString(3, user.name);
            pstmt.setString(4, user.surname);
            pstmt.setString(5, user.role);
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
            pstmt.setString(22, user.uid);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    /*    
    */
    @Override
    public List<User> getOrganizationUsers(long organizationId, Integer limit, Integer offset)
            throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path, tenant_users.tenant_id FROM users "
                + "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id "
                + "WHERE organization=? AND tenant_users.user_id IS NULL ";
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
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                users.add(buildUser(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

    @Override
    public List<User> getUsers(Integer limit, Integer offset) throws IotDatabaseException {
        // String query = "SELECT * from users";
        String query = "SELECT users.*, tenant_users.path, tenant_users.tenant_id FROM users "
                + "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id ";
        if (limit != null) {
            query += " LIMIT " + limit;
        }
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        ArrayList<User> users = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                users.add(buildUser(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

    @Override
    public void addUser(User user) throws IotDatabaseException {
        String query = "INSERT INTO users "
                + "(uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "services,phoneprefix,credits,autologin,language,organization, phone) "
                + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, user.uid);
            pstmt.setInt(2, user.type);
            pstmt.setString(3, user.email);
            pstmt.setString(4, user.name);
            pstmt.setString(5, user.surname);
            pstmt.setString(6, user.role);
            pstmt.setString(7, user.confirmString);
            pstmt.setString(8, user.password);
            pstmt.setString(9, user.generalNotificationChannel);
            pstmt.setString(10, user.infoNotificationChannel);
            pstmt.setString(11, user.warningNotificationChannel);
            pstmt.setString(12, user.alertNotificationChannel);
            pstmt.setBoolean(13, user.confirmed);
            pstmt.setBoolean(14, user.unregisterRequested);
            pstmt.setInt(15, user.authStatus);
            pstmt.setTimestamp(16, new java.sql.Timestamp(user.createdAt));
            pstmt.setInt(17, user.services);
            pstmt.setString(18, user.phonePrefix);
            pstmt.setLong(19, user.credits);
            pstmt.setBoolean(20, user.autologin);
            pstmt.setString(21, user.preferredLanguage);
            pstmt.setLong(22, user.organization);
            if (user.phone == null) {
                pstmt.setNull(23, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(23, user.phone);
            }
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
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
    public List<User> getTenantUsers(long tenantId, Integer limit, Integer offset) throws IotDatabaseException {
        String query = "SELECT users.*, tenant_users.path, tenant_users.tenant_id FROM users "
                + "LEFT JOIN tenant_users ON users.user_number=tenant_users.user_id "
                + "WHERE tenant_users.tenant_id=?";
        if (limit != null) {
            query += " LIMIT " + limit;
        }
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        ArrayList<User> users = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, tenantId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                users.add(buildUser(rs));
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

}
