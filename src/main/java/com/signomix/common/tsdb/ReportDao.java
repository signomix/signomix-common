package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.ReportDaoIface;

import io.agroal.api.AgroalDataSource;

/**
 * Implements ReportDaoIface for PostgreSQL database
 */
public class ReportDao implements ReportDaoIface{

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource=dataSource;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY reports to '/var/lib/postgresql/data/export/reports.csv' DELIMITER ';' CSV HEADER;";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        String query=
        "CREATE TABLE IF NOT EXISTS reports ("
        + "class_name VARCHAR,"
        + "organization INTEGER,"
        + "tenant INTEGER,"
        + "path LTREE,"
        + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"
        +")";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

    }

}
