package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.db.ApplicationDaoIface;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.iot.Application;

import io.agroal.api.AgroalDataSource;

public class ApplicationDao implements ApplicationDaoIface {

    public static final long DEFAULT_ORGANIZATION_ID = 1;

    private static final Logger logger = Logger.getLogger(ApplicationDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY applications to '/var/lib/postgresql/data/export/applications.csv' DELIMITER ';' CSV HEADER;";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("backupDb", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("backupDb", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        StringBuilder sb=new StringBuilder("");
        sb.append("CREATE SEQUENCE IF NOT EXISTS id_app_seq;");
        sb.append("create table IF NOT EXISTS applications (")
                .append("id bigint default id_app_seq.nextval primary key,")
                .append("organization bigint default "+DEFAULT_ORGANIZATION_ID+",")
                .append("version bigint default 0,")
                .append("name varchar UNIQUE, configuration varchar);");
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(sb.toString());) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        String indexQuery="CREATE INDEX IF NOT EXISTS idx_applications_name ON applications (name);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(indexQuery);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement("INSERT INTO applications values (0,"+DEFAULT_ORGANIZATION_ID+",0,'system','{}');");) {
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.error("createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public Application addApplication(Application application) throws IotDatabaseException {
        String query = "INSERT INTO APPLICATIONS (organization,version,name,configuration) values (?,?,?,?);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, application.organization);
            pst.setLong(2, application.version);
            pst.setString(3, application.name);
            pst.setString(4, application.configuration);
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.error("addApplication", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return getApplication(application.name);
    }

    @Override
    public void updateApplication(Application application) throws IotDatabaseException {
        String query = "UPDATE applications SET organization=?, version=?, name=?, configuration=? WHERE id=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, application.organization);
            pst.setLong(2, application.version);
            pst.setString(3, application.name);
            pst.setString(4, application.configuration);
            pst.setLong(5, application.id);
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.error("updateApplication", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
    }

    @Override
    public void removeApplication(long id) throws IotDatabaseException {
        String query = "DELETE FROM applications WHERE id=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, id);
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.error("removeApplication", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
    }

    @Override
    public Application getApplication(long id) throws IotDatabaseException {
        Application app = null;
        String query = "SELECT id,organization,version,name,configuration FROM applications WHERE id=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, id);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                app = new Application(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4), rs.getString(5));
            }
            rs.close();
        } catch (SQLException e) {
            logger.error("getApplication", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return app;
    }

    @Override
    public Application getApplication(String name) throws IotDatabaseException {
        Application app = null;
        String query = "SELECT id,organization,version,name,configuration FROM applications WHERE name=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, name);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                app = new Application(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4), rs.getString(5));
            }
            rs.close();
        } catch (SQLException e) {
            logger.error("getApplication", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return app;
    }

    @Override
    public List<Application> getApplications(int limit, int offset) throws IotDatabaseException {
        ArrayList<Application> result = new ArrayList<>();
        Application app = null;
        String query = "SELECT id,organization,version,name,configuration FROM applications LIMIT ? OFFSET ?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, limit);
            pst.setInt(2, offset);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                result.add(
                        new Application(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4), rs.getString(5)));
            }
            rs.close();
        } catch (SQLException e) {
            logger.error("getApplications", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return result;
    }

    @Override
    public List<Application> getApplications(long organizationId, int limit, int offset) throws IotDatabaseException {
        ArrayList<Application> result = new ArrayList<>();
        Application app = null;
        String query = "SELECT id,organization,version,name,configuration FROM applications WHERE organization=? LIMIT ? OFFSET ?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setInt(2, limit);
            pst.setInt(3, offset);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                result.add(
                        new Application(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4), rs.getString(5)));
            }
            rs.close();
        } catch (SQLException e) {
            logger.error("getApplications", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return result;
    }

}
