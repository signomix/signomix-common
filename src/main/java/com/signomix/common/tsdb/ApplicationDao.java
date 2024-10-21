package com.signomix.common.tsdb;

import com.signomix.common.db.ApplicationDaoIface;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.iot.Application;
import io.agroal.api.AgroalDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

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
        String query = "COPY applications to '/var/lib/postgresql/data/export/applications.csv' DELIMITER ';' CSV HEADER;"
                + "COPY application_config to '/var/lib/postgresql/data/export/application_config.csv' DELIMITER ';' CSV HEADER;";
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
        StringBuilder sb = new StringBuilder("");
        sb.append("CREATE SEQUENCE IF NOT EXISTS id_app_seq;");
        sb.append("create table IF NOT EXISTS applications (")
                .append("id bigint default id_app_seq.nextval primary key,")
                .append("organization bigint default " + DEFAULT_ORGANIZATION_ID + ",")
                .append("version bigint default 0,")
                .append("name varchar, description varchar, config VARCHAR, UNIQUE (organization, name));");
/*                 .append("CREATE TABLE IF NOT EXISTS application_config (")
                .append("app_id BIGINT,")
                .append("key VARCHAR(64),")
                .append("value VARCHAR);"); */
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sb.toString());) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("createStructure_1: "+e.getMessage());
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("createStructure_2: "+ e.getMessage());
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        String indexQuery = "CREATE INDEX IF NOT EXISTS idx_applications_name ON applications (name);";
                //+ "CREATE INDEX IF NOT EXISTS idx_application_config_app_id ON application_config (app_id);";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(indexQuery);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("createStructure3: "+e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(
                        "INSERT INTO applications values (0," + DEFAULT_ORGANIZATION_ID + ",0,'system','','');");) {
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.warn("createStructure: " + e.getMessage());
            //throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public Application addApplication(Application application) throws IotDatabaseException {
        String query = "INSERT INTO APPLICATIONS (organization,version,name,description,config) values (?,?,?,?,?);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, application.organization);
            pst.setLong(2, application.version);
            pst.setString(3, application.name);
            pst.setString(4, application.description);
            pst.setString(5, application.config.getAsString());
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.warn("addApplication: "+e.getMessage());
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return getApplication(application.name);
    }

    @Override
    public void updateApplication(Application application) throws IotDatabaseException {
        String query = "UPDATE applications SET organization=?, version=?, name=?, description=?, config=? WHERE id=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, application.organization);
            pst.setLong(2, application.version);
            pst.setString(3, application.name);
            pst.setString(4, application.description);
            pst.setString(5, application.config.getAsString());
            pst.setLong(6, application.id);
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
        String query = "SELECT id,organization,version,name,description,config FROM applications WHERE id=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, id);
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    app = new Application(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4),
                            rs.getString(5));
                    app.setConfig(rs.getString(6));
                }
            }
        } catch (SQLException e) {
            logger.error("getApplication", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return app;
    }

    @Override
    public Application getApplication(String name) throws IotDatabaseException {
        Application app = null;
        String query = "SELECT id,organization,version,name,description,config FROM applications WHERE name=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, name);
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    app = new Application(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4),
                            rs.getString(5));
                    app.setConfig(rs.getString(6));
                }
            }
        } catch (SQLException e) {
            logger.error("getApplication", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return app;
    }

    @Override
    public List<Application> getApplications(int limit, int offset) throws IotDatabaseException {
        ArrayList<Application> result = new ArrayList<>();
        String query = "SELECT id,organization,version,name,description,config FROM applications LIMIT ? OFFSET ?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, limit);
            pst.setInt(2, offset);
            try (ResultSet rs = pst.executeQuery();) {
                Application application;
                while (rs.next()) {
                    result.add(
                           application = new Application(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4),
                                    rs.getString(5)));
                    application.setConfig(rs.getString(6));
                    result.add(application);
                }
            }
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
        String query = "SELECT id,organization,version,name,description,config FROM applications WHERE organization=? LIMIT ? OFFSET ?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setInt(2, limit);
            pst.setInt(3, offset);
            try (ResultSet rs = pst.executeQuery();) {
                while (rs.next()) {
                    result.add(
                            app=new Application(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getString(4),
                                    rs.getString(5)));
                    app.setConfig(rs.getString(6));
                    result.add(app);
                }
            }
        } catch (SQLException e) {
            logger.error("getApplications", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return result;
    }

/*     @Override
    public ApplicationConfig getApplicationConfig(long applicationId) throws IotDatabaseException {
        ApplicationConfig config = new ApplicationConfig();
        String query = "SELECT key,value FROM application_config WHERE app_id=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, applicationId);
            try (ResultSet rs = pst.executeQuery();) {
                while (rs.next()) {
                    config.put(rs.getString(1), rs.getString(2));
                }
            }
        } catch (SQLException e) {
            logger.error("getApplicationConfig", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return config;
    }

    @Override
    public void setApplicationConfig(long applicationId, ApplicationConfig config) throws IotDatabaseException {
        String query = "DELETE FROM application_config WHERE app_id=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, applicationId);
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.error("setApplicationConfig", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        query = "INSERT INTO application_config (app_id,key,value) values (?,?,?);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            for (String key : config.keySet()) {
                pst.setLong(1, applicationId);
                pst.setString(2, key);
                pst.setString(3, config.get(key));
                pst.executeUpdate();
            }
        } catch (SQLException e) {
            logger.error("setApplicationConfig", e);
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
    } */

}
