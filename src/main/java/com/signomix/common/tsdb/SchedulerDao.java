package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.jboss.logging.Logger;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.SchedulerDaoIface;

import io.agroal.api.AgroalDataSource;

public class SchedulerDao implements SchedulerDaoIface {

    private static final Logger logger = Logger.getLogger(SchedulerDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource ds) {
        this.dataSource = ds;
    }

        @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY task_definition to '/var/lib/postgresql/data/export/task_definition.csv' DELIMITER ';' CSV HEADER;"
                + "COPY task_parameter to '/var/lib/postgresql/data/export/task_parameter.csv' DELIMITER ';' CSV HEADER;";
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
                // Table task_definition holds TaskDefinition objects
        String query = "CREATE TABLE IF NOT EXISTS task_definition ("
                + "id BIGSERIAL PRIMARY KEY,"
                + "type INT NOT NULL,"
                + "userid VARCHAR,"
                + "enabled BOOLEAN NOT NULL,"
                + "nl_schedule_definition VARCHAR(1024),"
                + "schedule_definition VARCHAR(255),"
                + "description VARCHAR(255),"
                + "organization INT"
                + ");";

        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new RuntimeException("Error creating table task_definition", e);
        }

        // Table task_parameter holds the parameters for the tasks
        String query2 = "CREATE TABLE IF NOT EXISTS task_parameter ("
                + "task_id BIGINT NOT NULL,"
                + "name VARCHAR(32) NOT NULL,"
                + "value VARCHAR(1024) NOT NULL"
                + ");";
        try (Connection connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query2);
        } catch (Exception e) {
            throw new RuntimeException("Error creating table task_parameter", e);
        }

    }

}
