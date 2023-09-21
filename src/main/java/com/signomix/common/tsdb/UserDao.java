package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.HashMaker;
import com.signomix.common.Organization;
import com.signomix.common.User;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.UserDaoIface;

import io.agroal.api.AgroalDataSource;

public class UserDao implements UserDaoIface {
    private static final Logger LOG = Logger.getLogger(UserDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource ds) {
        this.dataSource = ds;
    }

    public void createStructure() throws IotDatabaseException {
        String query;
        StringBuilder sb = new StringBuilder();
        // sb.append("create sequence if not exists user_number_seq;");
        // sb.append("create sequence if not exists org_number_seq;");
        sb.append("create table if not exists organizations (")
                .append("id SERIAL PRIMARY KEY,")
                .append("code varchar unique,")
                .append("name varchar,")
                .append("description varchar);");
        sb.append("create table if not exists users (")
                .append("uid varchar primary key,")
                .append("type int,")
                .append("email varchar,")
                .append("name varchar,")
                .append("surname varchar,")
                .append("role varchar,")
                .append("secret varchar,")
                .append("password varchar,")
                .append("generalchannel varchar,")
                .append("infochannel varchar,")
                .append("warningchannel varchar,")
                .append("alertchannel varchar,")
                .append("confirmed boolean,")
                .append("unregisterreq boolean,")
                .append("authstatus int,")
                .append("created timestamp,")
                .append("services int,")
                .append("phoneprefix varchar,")
                .append("credits bigint,")
                .append("user_number SERIAL,")
                .append("autologin boolean,")
                .append("language varchar,")
                .append("organization bigint default 1 references organizations(id));");
        query = sb.toString();
        LOG.info("Creating database structure: " + query);
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
        query = "insert into organizations (id,code,name,description) values (1,'','default','default organization - for accounts without organization feature');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e2) {
            LOG.warn("Error inserting default organization", e2);
            // throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
            // e2.getMessage(), e2);
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
        user.number = 0;
        user.services = 0;
        user.phonePrefix = "";
        user.credits = 0;
        user.autologin = false;
        user.preferredLanguage = "en";
        user.organization = 1;
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
        user.number = 0;
        user.services = 0;
        user.phonePrefix = "";
        user.credits = 0;
        user.autologin = false;
        user.preferredLanguage = "en";
        user.organization = 1;
        try {
            addUser(user);
        } catch (IotDatabaseException e) {
            LOG.warn("Error inserting default admin user", e);
        }

    }

    @Override
    public void removeNotConfirmed(long days) {
        long seconds = days * 24 * 60 * 60;
        // SELECT uid from users as u where extract(epoch from(current_timestamp-u.created)) > days*24*60*60 and confirmed=false;
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
        // uid,type,email,name,surname,role,secret,password,generalchannel,infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,number,services,phoneprefix,credits,autologin
        User user = new User();
        user.uid = rs.getString(1);
        user.type = rs.getInt(2);
        user.email = rs.getString(3);
        user.name = rs.getString(4);
        user.surname = rs.getString(5);
        user.role = rs.getString(6);
        user.confirmString = rs.getString(7);
        user.password = rs.getString(8);
        user.generalNotificationChannel = rs.getString(9);
        user.infoNotificationChannel = rs.getString(10);
        user.warningNotificationChannel = rs.getString(11);
        user.alertNotificationChannel = rs.getString(12);
        user.confirmed = rs.getBoolean(13);
        user.unregisterRequested = rs.getBoolean(14);
        user.authStatus = rs.getInt(15);
        user.createdAt = rs.getTimestamp(16).getTime();
        user.number = rs.getLong(17);
        user.services = rs.getInt(18);
        user.phonePrefix = rs.getString(19);
        user.credits = rs.getLong(20);
        user.autologin = rs.getBoolean(21);
        user.preferredLanguage = rs.getString(22);
        user.organization = rs.getLong(23);
        return user;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        // TODO: implement
        /*
         * String query = "CALL CSVWRITE('backup/users.csv', 'SELECT * FROM users');"
         * +
         * "CALL CSVWRITE('backup/organizations.csv', 'SELECT * FROM organizations');";
         * try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt =
         * conn.prepareStatement(query);) {
         * pstmt.execute();
         * } catch (SQLException e) {
         * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
         * e.getMessage(), e);
         * } catch (Exception e) {
         * throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
         * }
         */
    }

    @Override
    public User getUser(String uid) throws IotDatabaseException {
        String query = "SELECT uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "user_number,services,phoneprefix,credits,autologin,language,organization from users "
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
        String query = "SELECT uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "user_number,services,phoneprefix,credits,autologin,language,organization from users "
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
        String query = "SELECT uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "user_number,services,phoneprefix,credits,autologin,language,organization from users "
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
        String query = "SELECT uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "user_number,services,phoneprefix,credits,autologin,language,organization from users "
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
                + "type=?,email=?,name=?,surname=?,role=?,secret=?,password=?,generalchannel=?,"
                + "infochannel=?,warningchannel=?,alertchannel=?,confirmed=?,unregisterreq=?,authstatus=?,created=?,"
                + "services=?,phoneprefix=?,credits=?,autologin=?,language=?,organization=? "
                + "WHERE uid=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setInt(1, user.type);
            pstmt.setString(2, user.email);
            pstmt.setString(3, user.name);
            pstmt.setString(4, user.surname);
            pstmt.setString(5, user.role);
            pstmt.setString(6, user.confirmString);
            pstmt.setString(7, user.password);
            pstmt.setString(8, user.generalNotificationChannel);
            pstmt.setString(9, user.infoNotificationChannel);
            pstmt.setString(10, user.warningNotificationChannel);
            pstmt.setString(11, user.alertNotificationChannel);
            pstmt.setBoolean(12, user.confirmed);
            pstmt.setBoolean(13, user.unregisterRequested);
            pstmt.setInt(14, user.authStatus);
            pstmt.setTimestamp(15, new java.sql.Timestamp(user.createdAt));
            pstmt.setInt(16, user.services);
            pstmt.setString(17, user.phonePrefix);
            pstmt.setLong(18, user.credits);
            pstmt.setBoolean(19, user.autologin);
            pstmt.setString(20, user.preferredLanguage);
            pstmt.setLong(21, user.organization);
            pstmt.setString(22, user.uid);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public List<Organization> getOrganizations(Integer limit, Integer offset) throws IotDatabaseException {
        String query = "SELECT id,code,name,description from organizations";
        if (limit != null) {
            query += " LIMIT " + limit;
        }
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        List<Organization> orgs = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                Organization org = new Organization(
                        rs.getLong("id"),
                        rs.getString("code"),
                        rs.getString("name"),
                        rs.getString("description"));
                orgs.add(org);
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return orgs;
    }

    @Override
    public Organization getOrganization(long id) throws IotDatabaseException {
        String query = "SELECT id,code,name,description from organizations where id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Organization org = new Organization(
                        rs.getLong("id"),
                        rs.getString("code"),
                        rs.getString("name"),
                        rs.getString("description"));
                return org;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return null;
    }

    @Override
    public Organization getOrganization(String code) throws IotDatabaseException {
        String query = "SELECT id,code,name,description from organizations where code=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, code);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Organization org = new Organization(
                        rs.getLong("id"),
                        rs.getString("code"),
                        rs.getString("name"),
                        rs.getString("description"));
                return org;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return null;
    }

    @Override
    public void addOrganization(Organization org) throws IotDatabaseException {
        String query = "INSERT INTO organizations (code,name,description) VALUES (?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, org.code);
            pstmt.setString(2, org.name);
            pstmt.setString(3, org.description);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void updateOrganization(Organization org) throws IotDatabaseException {
        String query = "UPDATE organizations SET code=?,name=?,description=? WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, org.code);
            pstmt.setString(2, org.name);
            pstmt.setString(3, org.description);
            pstmt.setLong(4, org.id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void deleteOrganization(long id) throws IotDatabaseException {
        String query = "DELETE FROM organizations WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public List<User> getOrganizationUsers(long organizationId, Integer limit, Integer offset)
            throws IotDatabaseException {
        String query = "SELECT uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "user_number,services,phoneprefix,credits,autologin,language,organization from users "
                + "WHERE organization=?";
        if (limit != null) {
            query += " LIMIT " + limit;
        }
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        ArrayList<User> users = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, organizationId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                users.add(buildUser(rs));
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

    @Override
    public List<User> getUsers(Integer limit, Integer offset) throws IotDatabaseException {
        String query = "SELECT uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "user_number,services,phoneprefix,credits,autologin,language,organization from users";
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
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return users;
    }

    @Override
    public void addUser(User user) throws IotDatabaseException {
        String query = "INSERT INTO users "
                + "(uid,type,email,name,surname,role,secret,password,generalchannel,"
                + "infochannel,warningchannel,alertchannel,confirmed,unregisterreq,authstatus,created,"
                + "services,phoneprefix,credits,autologin,language,organization) "
                + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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

}
