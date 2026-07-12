package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.signomix.common.User;
import com.signomix.common.db.EventLogDaoIface;
import com.signomix.common.db.IotDatabaseException;

import io.agroal.api.AgroalDataSource;
import org.jboss.logging.Logger;

public class EventLogDao implements EventLogDaoIface {

    private static final Logger logger = Logger.getLogger(EventLogDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void createStructure() throws IotDatabaseException {

        String query = "CREATE TYPE event_type AS ENUM ('login', 'logout', 'query', 'insert', 'adminlogin');";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
        }

        query = "CREATE TABLE IF NOT EXISTS data_access_events ("
                + "ts TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                + "uid VARCHAR,"
                + "organization_id BIGINT"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "createStructure1 " + e.getMessage());
        }
        query = "SELECT create_hypertable('data_access_events', 'ts',migrate_data => true);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }

        query = "CREATE TABLE IF NOT EXISTS account_events ("
                + "ts TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                + "uid VARCHAR,"
                + "organization_id BIGINT,"
                + "client_ip VARCHAR(20),"
                + "event_type event_type,"
                + "error_code INTEGER"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "createStructure2 " + e.getMessage());
        }
        query = "SELECT create_hypertable('account_events', 'ts',migrate_data => true);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }

        query = "CREATE TABLE IF NOT EXISTS api_events ("
                + "ts TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                + "uid VARCHAR,"
                + "organization_id BIGINT,"
                + "client_ip VARCHAR(20),"
                + "event_type event_type,"
                + "error_code INTEGER"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "createStructure2 " + e.getMessage());
        }
        query = "SELECT create_hypertable('api_events', 'ts',migrate_data => true);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }

        // remove retention policy
        query="SELECT remove_retention_policy('api_events');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }
        query="SELECT remove_retention_policy('account_events');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }
        query="SELECT remove_retention_policy('data_access_events');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }

        query="SELECT add_retention_policy('api_events', INTERVAL '1 month');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }
        query="SELECT add_retention_policy('account_events', INTERVAL '1 month');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }
        query="SELECT add_retention_policy('data_access_events', INTERVAL '1 month');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }

    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY (SELECT * FROM data_access_events) TO '/var/lib/postgresql/data/export/data_access_events.csv' DELIMITER ';' CSV HEADER;"
                + "COPY (SELECT * FROM account_events) TO '/var/lib/postgresql/data/export/account_events.csv' DELIMITER ';' CSV HEADER;"
                + "COPY (SELECT * FROM api_events) TO '/var/lib/postgresql/data/export/api_events.csv' DELIMITER ';' CSV HEADER;";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "backupDb " + e.getMessage());
        }

    }

    @Override
    public void restoreDb() throws IotDatabaseException {
        logger.info("EventLogDao.restoreDb");
        //clear tables
        String query = "DELETE FROM data_access_events;"
                + "DELETE FROM account_events;"
                + "DELETE FROM api_events;";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "restoreDb " +e.getMessage());
        }
        query = "COPY data_access_events FROM '/var/lib/postgresql/data/import/data_access_events.csv' DELIMITER ';' CSV HEADER;"
                + "COPY account_events FROM '/var/lib/postgresql/data/import/account_events.csv' DELIMITER ';' CSV HEADER;"
                + "COPY api_events FROM '/var/lib/postgresql/data/import/api_events.csv' DELIMITER ';' CSV HEADER;";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "restoreDb " + e.getMessage());
        }
        logger.info("EventLogDao.restoreDb done");
    }

    @Override
    public void deletePartition(String partition, int monthsBack) throws IotDatabaseException {
    }

    @Override
    public void saveLoginEvent(User user, String remoteAddress, int resultCode, boolean isAdmin) {
        String query = "INSERT INTO account_events (uid, organization_id, client_ip, event_type, error_code) VALUES (?, ?, ?, ?, ?)";
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(query)) {
            statement.setString(1, user.uid);
            statement.setLong(2, user.organization);
            statement.setString(3, remoteAddress);
            statement.setObject(4, isAdmin ? "adminlogin" : "login", java.sql.Types.OTHER);
            statement.setInt(5, resultCode);
            statement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveLoginFailure(String login, String remoteAddress, int resultCode, boolean isAdmin) {
        String query = "INSERT INTO account_events (uid, client_ip, event_type, error_code) VALUES (?, ?, ?, ?)";
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(query)) {
            statement.setString(1, login);
            statement.setString(2, remoteAddress);
            statement.setObject(3, isAdmin ? "adminlogin" : "login", java.sql.Types.OTHER);
            statement.setInt(4, resultCode);
            statement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
