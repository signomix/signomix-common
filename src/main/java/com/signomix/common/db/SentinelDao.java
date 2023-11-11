package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.inject.Inject;

import org.jboss.logging.Logger;

import com.signomix.common.iot.sentry.SentinelConfig;

import io.agroal.api.AgroalDataSource;

public class SentinelDao implements SentinelDaoIface {

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
        String query = "CALL CSVWRITE('backup/alert_definitions.csv', 'SELECT * FROM alert_definitions');"
                + "CALL CSVWRITE('backup/alert_events.csv', 'SELECT * FROM alert_events');";
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
    public void addConfig(SentinelConfig config) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addConfig'");
    }

    @Override
    public void updateConfig(SentinelConfig config) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateConfig'");
    }

    @Override
    public void removeConfig(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeConfig'");
    }

    @Override
    public SentinelConfig getConfig(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfig'");
    }

    @Override
    public List<SentinelConfig> getConfigs(long userId, int limit, int offset) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigs'");
    }

    @Override
    public List<SentinelConfig> getOrganizationConfigs(long organizationId, int limit, int offset)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrganizationConfigs'");
    }

    @Override
    public List<SentinelConfig> getConfigsByDevice(String deviceEui, int limit, int offset)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigsByDevice'");
    }


}
