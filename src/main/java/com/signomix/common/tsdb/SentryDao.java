package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.inject.Inject;

import org.jboss.logging.Logger;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.SentryDaoIface;
import com.signomix.common.iot.sentry.SentryConfig;

import io.agroal.api.AgroalDataSource;

public class SentryDao implements SentryDaoIface {

    public static final long DEFAULT_ORGANIZATION_ID = 1;

    @Inject
    Logger logger;

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "CALL CSVWRITE('backup/sentry_configs.csv', 'SELECT * FROM sentry_configs');"
                + "CALL CSVWRITE('backup/sentry_events.csv', 'SELECT * FROM sentry_events');"
                + "CALL CSVWRITE('backup/sentry_devices.csv', 'SELECT * FROM sentry_devices');";
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
        throw new UnsupportedOperationException("Unimplemented method 'createStructure'");
    }

    @Override
    public void addConfig(SentryConfig config) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addConfig'");
    }

    @Override
    public void updateConfig(SentryConfig config) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateConfig'");
    }

    @Override
    public void removeConfig(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeConfig'");
    }

    @Override
    public SentryConfig getConfig(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfig'");
    }

    @Override
    public List<SentryConfig> getConfigs(long userId, int limit, int offset) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigs'");
    }

    @Override
    public List<SentryConfig> getOrganizationConfigs(long organizationId, int limit, int offset)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrganizationConfigs'");
    }

}
