package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jboss.logging.Logger;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.ReportDaoIface;

import io.agroal.api.AgroalDataSource;

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
        String query = "COPY reports to '/var/lib/postgresql/data/export/reports.csv' DELIMITER ';' CSV HEADER;";
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
                + "user_id INTEGER,"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "UNIQUE NULLS NOT DISTINCT (class_name, organization, tenant, path, user_id)"
                + ")";
        String query2 = "CREATE INDEX IF NOT EXISTS reports_idx ON reports (path);"
                + "CREATE INDEX IF NOT EXISTS reports_idx2 ON reports (class_name);"
                + "CREATE INDEX IF NOT EXISTS reports_idx3 ON reports (organization);"
                + "CREATE INDEX IF NOT EXISTS reports_idx4 ON reports (tenant);"
                + "CREATE INDEX IF NOT EXISTS reports_idx5 ON reports (user_id);";

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
        if (userNumber != null) {
            String query2 = "SELECT COUNT(*) FROM reports WHERE class_name=? AND (user_id=? OR user_id=?);";
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query2);) {
                pstmt.setString(1, className);
                pstmt.setLong(2, userNumber == null ? 0L : userNumber);
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
        if (organization != null && path != null && !path.isEmpty()) {
            if (tenant == null) {
                String query = "SELECT COUNT(*) FROM reports WHERE class_name=? AND organization=? AND path ~ ?;";
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(query);) {
                    pstmt.setString(1, className);
                    pstmt.setInt(2, organization);
                    pstmt.setObject(3, path, java.sql.Types.OTHER);
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
            } else {
                String query = "SELECT COUNT(*) FROM reports WHERE class_name=? AND organization=? AND tenant=? AND path ~ ?;";
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(query);) {
                    pstmt.setString(1, className);
                    pstmt.setInt(2, organization);
                    pstmt.setInt(3, tenant);
                    pstmt.setObject(4, path, java.sql.Types.OTHER);
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

            }
        }
        return isAvailable;
    }

    @Override
    public void saveReport(String className, Long userNumber, Integer organization, Integer tenant, String path)
            throws IotDatabaseException {
        String query = "INSERT INTO reports (class_name, organization, tenant, path, user_id) VALUES (?,?,?,?,?) "
                + "ON CONFLICT (class_name, organization, tenant, path, user_id) DO NOTHING;";
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
            logger.error("Error during saveReport", e);
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during saveReport", e);
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

}
