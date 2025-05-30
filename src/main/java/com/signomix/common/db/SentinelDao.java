package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.jboss.logging.Logger;

import com.signomix.common.iot.sentinel.SentinelConfig;

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
        String query = "CALL CSVWRITE('backup/sentinelss.csv', 'SELECT * FROM sentinelss');"
                + "CALL CSVWRITE('backup/sentinel_events.csv', 'SELECT * FROM sentinel_events');"
                + "CALL CSVWRITE('backup/sentinel_devices.csv', 'SELECT * FROM sentinel_devices');";
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
    public long addConfig(SentinelConfig config) throws IotDatabaseException {
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
    public List<SentinelConfig> getConfigs(String userId, int limit, int offset, int type) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigs'");
    }

    @Override
    public List<SentinelConfig> getOrganizationConfigs(long organizationId, int limit, int offset, int type)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrganizationConfigs'");
    }

    @Override
    public List<SentinelConfig> getConfigsByDevice(String deviceEui, int limit, int offset, int type)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigsByDevice'");
    }

    public List<List> getLastValuesByConfigId(long sentinelConfigId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLastValuesByConfigId'");
    }

    @Override
    public void addDevice(long configId, String deviceEui, String channelMapping) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addDevice'");
    }

    @Override
    public void removeConfigDevice(long configId, String deviceEui) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeDevice'");
    }

    @Override
    public Map<String, String> getDevices(long configId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDevices'");
    }

    @Override
    public void addSentinelEvent(long configId, String deviceEui, int level, String message_pl, String message_en)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addSentinelEvent'");
    }

    @Override
    public void removeDevices(long configId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeDevices'");
    }

    @Override
    public int getSentinelStatus(long configId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSentinelStatus'");
    }

    @Override
    public List<SentinelConfig> getConfigsByTag(String tagName, String tagValue, int limit, int offse, int type)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigsByTag'");
    }

    @Override
    public List<SentinelConfig> getConfigsByGroup(String groupName, int limit, int offset, int type) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getConfigsByGroup'");
    }

    @Override
    public List<List> getLastValuesByDeviceEui(String deviceEui) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLastValuesByDeviceEui'");
    }

    @Override
    public void removeDevice(String deviceEui) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeDevice'");
    }

    @Override
    public Map<String, Map<String, String>> getDeviceChannelsByConfigId(long configId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDeviceChannelsByConfigId'");
    }

    @Override
    public List<List> getLastValuesOfDevices(Set<String> euis, long secondsBack) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLastValuesOfDevices'");
    }

    @Override
    public Map<String, Map<String, String>> getDeviceChannelsByConfigAndEui(long configId, String eui) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDeviceChannelsByEui'");
    }


}
