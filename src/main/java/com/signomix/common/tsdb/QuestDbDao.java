package com.signomix.common.tsdb;

import com.signomix.common.User;
import com.signomix.common.db.EventLogDaoIface;
import com.signomix.common.db.IotDatabaseException;
import io.agroal.api.AgroalDataSource;

public class QuestDbDao implements EventLogDaoIface {

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        String query = "CREATE TABLE IF NOT EXISTS data_access_events ("
                + "ts TIMESTAMP, "
                + "uid SYMBOL,"
                + "organization_id LONG"
                + ") timestamp(ts) PARTITION BY DAY;";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "createStructure1 " + e.getMessage());
        }

        query = "CREATE TABLE IF NOT EXISTS account_events ("
                + "ts TIMESTAMP, "
                + "uid SYMBOL,"
                + "organization_id LONG,"
                + "client_ip STRING,"
                + "event_type SYMBOL,"
                + "error_code INT"
                + ") timestamp(ts) PARTITION BY DAY;";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "createStructure2 " + e.getMessage());
        }

    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY data_access_events TO '/var/lib/postgresql/data/export/qdb_data_access_events.csv' DELIMITER ';' CSV HEADER;"
                + "COPY account_events TO '/var/lib/postgresql/data/export/qdb_account_events.csv' DELIMITER ';' CSV HEADER;";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.execute(query);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "backupDb " + e.getMessage());
        }

    }

    @Override
    public void deletePartition(String partition, int monthsBack) throws IotDatabaseException {
        String query = "ALTER TABLE ? "
                + "DROP PARTITION "
                + "WHERE timestamp < dateadd('M', -1*?, systimestamp());";
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(query)) {
            statement.setString(1, partition);
            statement.setInt(2, monthsBack);
            statement.execute();
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, "deletePartition " + e.getMessage());
        }
    }

    @Override
    public void saveLoginEvent(User user, String remoteAddress, int resultCode, boolean isAdmin) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'saveLoginEvent'");
    }

    @Override
    public void saveLoginFailure(String login, String remoteAddress, int resultCode, boolean isAdmin) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'saveLoginFailure'");
    }

}
