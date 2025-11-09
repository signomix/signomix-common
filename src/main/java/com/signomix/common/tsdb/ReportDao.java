package com.signomix.common.tsdb;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.ReportDaoIface;
import com.signomix.common.db.ReportDefinition;
import io.agroal.api.AgroalDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * Implements ReportDaoIface for PostgreSQL database
 */
public class ReportDao implements ReportDaoIface {

    private static final Logger logger = Logger.getLogger(ReportDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY reports to '/var/lib/postgresql/data/export/reports.csv' DELIMITER ';' CSV HEADER;"
                + "COPY report_definitions to '/var/lib/postgresql/data/export/report_definitions.csv' DELIMITER ';' CSV HEADER;";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error during backup", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during backup", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        String query = "CREATE TABLE IF NOT EXISTS reports ("
                + "class_name VARCHAR,"
                + "organization INTEGER,"
                + "tenant INTEGER,"
                + "path LTREE,"
                + "userid INTEGER,"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "UNIQUE NULLS NOT DISTINCT (class_name, organization, tenant, path, userid)"
                + ")";
        String query2 = "CREATE INDEX IF NOT EXISTS reports_idx ON reports (path);"
                + "CREATE INDEX IF NOT EXISTS reports_idx2 ON reports (class_name);"
                + "CREATE INDEX IF NOT EXISTS reports_idx3 ON reports (organization);"
                + "CREATE INDEX IF NOT EXISTS reports_idx4 ON reports (tenant);"
                + "CREATE INDEX IF NOT EXISTS reports_idx5 ON reports (userid);";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error during createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query2);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error during createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

        // create report_definitons table
        String query3 = "CREATE TABLE IF NOT EXISTS report_definitions ("
                + "id SERIAL PRIMARY KEY,"
                + "organization INTEGER,"
                + "tenant INTEGER,"
                + "path LTREE,"
                + "userid VARCHAR,"
                + "team VARCHAR,"
                + "administrators VARCHAR,"
                + "dql VARCHAR,"
                + "name VARCHAR,"
                + "description VARCHAR,"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"
                + ")";
        String query4 = "CREATE INDEX IF NOT EXISTS report_definitions_idx ON report_definitions (path);"
                + "CREATE INDEX IF NOT EXISTS report_definitions_idx2 ON report_definitions (organization);"
                + "CREATE INDEX IF NOT EXISTS report_definitions_idx3 ON report_definitions (tenant);"
                + "CREATE INDEX IF NOT EXISTS report_definitions_idx4 ON report_definitions (userid);";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query3);) {
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error during createStructure: "+ e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during createStructure", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

    }

    /**
     * Check if a report is available for a given user
     * In case userNumber is null, check if a report is available for a given
     * organization, tenant and path.
     * 
     * @param className
     * @param userNumber
     * @param organization
     * @param tenant
     * @param path
     * @return boolean true if the report is available
     */
    @Override
    public boolean isAvailable(String className, Long userNumber, Integer organization, Integer tenant, String path)
            throws IotDatabaseException {
        boolean isAvailable = false;
        logger.info("input parameters: className="+className+", userNumber="+userNumber+", organization="+organization+", tenant="+tenant+", path="+path);
        if (userNumber != null) {
            logger.debug("Checking report availability for user: " + userNumber+" class: "+className); ;
            String query2 = "SELECT COUNT(*) FROM reports WHERE class_name=? AND (userid=? OR userid=?);";
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query2);) {
                pstmt.setString(1, className);
                pstmt.setLong(2, userNumber);
                pstmt.setLong(3, 0);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        isAvailable = rs.getInt(1) > 0;
                    }
                }
            } catch (SQLException e) {
                logger.error("Error during isAvailable", e);
                e.printStackTrace();
            }
            if (isAvailable) {
                return true;
            }
        }
        if (organization != null) {
            //TODO: implement path and tenant checking
            /* if (tenant == null) {
                String query = "SELECT COUNT(*) FROM reports WHERE class_name=? AND organization=?;";
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(query);) {
                    pstmt.setString(1, className);
                    pstmt.setInt(2, organization);
                    //pstmt.setObject(3, path, java.sql.Types.OTHER);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            isAvailable = rs.getInt(1) > 0;
                        }
                    }
                } catch (SQLException e) {
                    logger.error("Error during isAvailable", e);
                    e.printStackTrace();
                } catch (Exception e) {
                    logger.error("Error during isAvailable", e);
                    e.printStackTrace();
                }
            } else { */
                String query = "SELECT COUNT(*) FROM reports WHERE class_name=? AND (organization=? OR userid = 0)";
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(query);) {
                    pstmt.setString(1, className);
                    pstmt.setInt(2, organization);
                    //pstmt.setInt(3, tenant);
                    //pstmt.setObject(4, path, java.sql.Types.OTHER);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            isAvailable = rs.getInt(1) > 0;
                        }
                    }
                } catch (SQLException e) {
                    logger.error("Error during isAvailable", e);
                    e.printStackTrace();
                } catch (Exception e) {
                    logger.error("Error during isAvailable", e);
                    e.printStackTrace();
                }

            //}
        }
        return isAvailable;
    }

    @Override
    public void saveReport(String className, Long userNumber, Integer organization, Integer tenant, String path)
            throws IotDatabaseException {
        String query = "INSERT INTO reports (class_name, organization, tenant, path, userid) VALUES (?,?,?,?,?); ";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, className);
            if (organization == null) {
                pstmt.setNull(2, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(2, organization);
            }
            if (tenant == null) {
                pstmt.setNull(3, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(3, tenant);
            }
            if (path == null) {
                pstmt.setNull(4, java.sql.Types.OTHER);
            } else {
                pstmt.setString(4, path);
            }
            if (userNumber == null) {
                pstmt.setNull(5, java.sql.Types.INTEGER);
            } else {
                pstmt.setLong(5, userNumber);
            }
            pstmt.execute();
        } catch (SQLException e) {
            logger.warn("Error during saveReport1: " +e.getMessage());
            //throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during saveReport2"+e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void saveReportDefinition(ReportDefinition reportDefinition) throws IotDatabaseException {
        String query = "INSERT INTO report_definitions (organization, tenant, path, userid, team, administrators, dql, name, description) VALUES (?,?,?,?,?,?,?,?,?); ";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            if (reportDefinition.organization == null) {
                pstmt.setNull(1, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(1, reportDefinition.organization);
            }
            if (reportDefinition.tenant == null) {
                pstmt.setNull(2, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(2, reportDefinition.tenant);
            }
            if (reportDefinition.path == null) {
                pstmt.setNull(3, java.sql.Types.OTHER);
            } else {
                pstmt.setString(3, reportDefinition.path);
            }
            if (reportDefinition.userLogin == null) {
                pstmt.setNull(4, java.sql.Types.INTEGER);
            } else {
                pstmt.setString(4, reportDefinition.userLogin);
            }
            if(reportDefinition.team == null){
                pstmt.setNull(5, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(5, reportDefinition.team);
            }
            if(reportDefinition.administrators == null){
                pstmt.setNull(6, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(6, reportDefinition.administrators);
            }
            if (reportDefinition.dql == null) {
                pstmt.setNull(5, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(5, reportDefinition.dql);
            }
            if (reportDefinition.name == null) {
                pstmt.setNull(6, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(6, reportDefinition.name);
            }
            if (reportDefinition.description == null) {
                pstmt.setNull(7, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(7, reportDefinition.description);
            }
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error during saveReportDefinition", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during saveReportDefinition", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public ReportDefinition getReportDefinition(Integer id) throws IotDatabaseException {
        String query = "SELECT * FROM report_definitions WHERE id=?;";
        ReportDefinition reportDefinition = null;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setInt(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    reportDefinition = new ReportDefinition();
                    reportDefinition.id = rs.getInt("id");
                    reportDefinition.organization = rs.getInt("organization");
                    reportDefinition.tenant = rs.getInt("tenant");
                    reportDefinition.path = rs.getString("path");
                    reportDefinition.userLogin = rs.getString("userid");
                    reportDefinition.team = rs.getString("team");
                    reportDefinition.administrators = rs.getString("administrators");
                    reportDefinition.dql = rs.getString("dql");
                    reportDefinition.name = rs.getString("name");
                    reportDefinition.description = rs.getString("description");
                }
                return reportDefinition;
            }
        } catch (SQLException e) {
            logger.error("Error during getReportDefinition", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during getReportDefinition", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public boolean deleteReportDefinition(Integer id) throws IotDatabaseException {
        String query = "DELETE FROM report_definitions WHERE id=?;";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setInt(1, id);
            return pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error during deleteReportDefinition", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during deleteReportDefinition", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void updateReportDefinition(Integer id, ReportDefinition reportDefinition) throws IotDatabaseException {
        String query = "UPDATE report_definitions SET organization=?, tenant=?, path=?, userid=?, team=?, administrators=?, dql=?, name=?, description=? WHERE id=?;";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            if (reportDefinition.organization == null) {
                pstmt.setNull(1, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(1, reportDefinition.organization);
            }
            if (reportDefinition.tenant == null) {
                pstmt.setNull(2, java.sql.Types.INTEGER);
            } else {
                pstmt.setInt(2, reportDefinition.tenant);
            }
            if (reportDefinition.path == null) {
                pstmt.setNull(3, java.sql.Types.OTHER);
            } else {
                pstmt.setString(3, reportDefinition.path);
            }
            if (reportDefinition.userLogin == null) {
                pstmt.setNull(4, java.sql.Types.INTEGER);
            } else {
                pstmt.setString(4, reportDefinition.userLogin);
            }
            if(reportDefinition.team == null){
                pstmt.setNull(5, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(5, reportDefinition.team);
            }
            if(reportDefinition.administrators == null){
                pstmt.setNull(6, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(6, reportDefinition.administrators);
            }
            if (reportDefinition.dql == null) {
                pstmt.setNull(7, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(7, reportDefinition.dql);
            }
            if (reportDefinition.name == null) {
                pstmt.setNull(8, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(8, reportDefinition.name);
            }
            if (reportDefinition.description == null) {
                pstmt.setNull(9, java.sql.Types.VARCHAR);
            } else {
                pstmt.setString(9, reportDefinition.description);
            }
            pstmt.setInt(10, id);
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("Error during updateReportDefinition", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during updateReportDefinition", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public List<ReportDefinition> getReportDefinitions(String userLogin) throws IotDatabaseException {
        String query = "SELECT * FROM report_definitions WHERE userid=? or report_definitions.team LIKE ? "+
        "OR report_definition.administrators LIKE ? ORDER BY name DESC;";

        ArrayList<ReportDefinition> reportDefinitions = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userLogin);
            pstmt.setString(2, "%,"+userLogin+",%");
            pstmt.setString(3, "%,"+userLogin+",%");
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    ReportDefinition reportDefinition = new ReportDefinition();
                    reportDefinition.id = rs.getInt("id");
                    reportDefinition.organization = rs.getInt("organization");
                    reportDefinition.tenant = rs.getInt("tenant");
                    reportDefinition.path = rs.getString("path");
                    reportDefinition.userLogin = rs.getString("userid");
                    reportDefinition.team = rs.getString("team");
                    reportDefinition.administrators = rs.getString("administrators");
                    reportDefinition.dql = rs.getString("dql");
                    reportDefinition.name = rs.getString("name");
                    reportDefinition.description = rs.getString("description");
                    reportDefinitions.add(reportDefinition);
                }
                return reportDefinitions;
            }
        } catch (SQLException e) {
            logger.error("Error during getReportDefinitions", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during getReportDefinitions", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    

}
