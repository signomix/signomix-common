package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.inject.Inject;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.SentinelDaoIface;
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
        String query = "COPY sentinels to '/var/lib/postgresql/data/export/sentinels.csv' DELIMITER ';' CSV HEADER;"
                + "COPY sentinel_events to '/var/lib/postgresql/data/export/sentinel_events.csv' DELIMITER ';' CSV HEADER;"
                + "COPY sentinel_devices to '/var/lib/postgresql/data/export/sentinel_devices.csv' DELIMITER ';' CSV HEADER;";
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
        String query = "CREATE TABLE IF NOT EXISTS sentinels ("
                + "id BIGSERIAL PRIMARY KEY,"
                + "tstamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + "name VARCHAR(255) NOT NULL,"
                + "active BOOLEAN NOT NULL,"
                + "user_id BIGINT NOT NULL,"
                + "organization_id BIGINT NOT NULL,"
                + "type INTEGER NOT NULL,"
                + "device_eui VARCHAR(255),"
                + "group_eui VARCHAR(255),"
                + "tag_name VARCHAR(255),"
                + "tag_value VARCHAR(255),"
                + "alert_level INTEGER NOT NULL,"
                + "alert_message VARCHAR(255),"
                + "every_time BOOLEAN NOT NULL,"
                + "condition_ok_message BOOLEAN NOT NULL,"
                + "conditions JSON NOT NULL DEFAULT '[]'"
                + ");"
                + "CREATE TABLE IF NOT EXISTS sentinel_events ("
                + "id BIGSERIAL PRIMARY KEY,"
                + "sentinel_id BIGINT NOT NULL,"
                + "tstamp TIMESTAMPTZ NOT NULL,"
                + "level INTEGER NOT NULL,"
                + "message VARCHAR(255) NOT NULL"
                + ");"
                + "CREATE TABLE IF NOT EXISTS sentinel_devices ("
                + "sentinel_id BIGINT,"
                + "eui VARCHAR(255) NOT NULL,"
                + ");";

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
    public void addConfig(SentinelConfig config) throws IotDatabaseException {
        String query = "INSERT INTO sentinels (name, active, user_id, organization_id, type, device_eui, group, tag_name, tag_value, alert_level, alert_message, alert_ok, condition_ok_message, conditions) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?::json)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, config.name);
            pstmt.setBoolean(2, config.active);
            pstmt.setLong(3, config.userId);
            pstmt.setLong(4, config.organizationId);
            pstmt.setInt(5, config.type);
            pstmt.setString(6, config.deviceEui);
            pstmt.setString(7, config.groupEui);
            pstmt.setString(8, config.tagName);
            pstmt.setString(9, config.tagValue);
            pstmt.setInt(10, config.alertLevel);
            pstmt.setString(11, config.alertMessage);
            pstmt.setBoolean(12, config.everyTime);
            pstmt.setBoolean(13, config.conditionOkMessage);
            pstmt.setObject(14, new ObjectMapper().writeValueAsString(config.conditions));
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void updateConfig(SentinelConfig config) throws IotDatabaseException {
        String query = "UPDATE sentinels SET name=?, active=?, user_id=?, organization_id=?, type=?, device_eui=?, group_eui=?, tag_name=?, tag_value=?, alert_level=?, alert_message=?, alert_ok=?, condition_ok_message=?, conditions=?::json WHERE id=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, config.name);
            pstmt.setBoolean(2, config.active);
            pstmt.setLong(3, config.userId);
            pstmt.setLong(4, config.organizationId);
            pstmt.setInt(5, config.type);
            pstmt.setString(6, config.deviceEui);
            pstmt.setString(7, config.groupEui);
            pstmt.setString(8, config.tagName);
            pstmt.setString(9, config.tagValue);
            pstmt.setInt(10, config.alertLevel);
            pstmt.setString(11, config.alertMessage);
            pstmt.setBoolean(12, config.everyTime);
            pstmt.setBoolean(13, config.conditionOkMessage);
            pstmt.setObject(14, new ObjectMapper().writeValueAsString(config.conditions));
            pstmt.setLong(15, config.id);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void removeConfig(long id) throws IotDatabaseException {
        String query = "DELETE FROM sentinels WHERE id=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public SentinelConfig getConfig(long id) throws IotDatabaseException {
        String query = "SELECT * FROM sentinels WHERE id=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    SentinelConfig config = new SentinelConfig();
                    config.id = rs.getLong("id");
                    config.name = rs.getString("name");
                    config.active = rs.getBoolean("active");
                    config.userId = rs.getLong("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOkMessage = rs.getBoolean("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    return config;
                }
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return null;
    }

    @Override
    public List<SentinelConfig> getConfigs(long userId, int limit, int offset) throws IotDatabaseException {
        String query = "SELECT * FROM sentinels WHERE user_id=? ORDER BY id DESC LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, userId);
            pstmt.setInt(2, limit);
            pstmt.setInt(3, offset);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                java.util.ArrayList<SentinelConfig> configs = new java.util.ArrayList<>();
                while (rs.next()) {
                    SentinelConfig config = new SentinelConfig();
                    config.id = rs.getLong("id");
                    config.name = rs.getString("name");
                    config.active = rs.getBoolean("active");
                    config.userId = rs.getLong("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOkMessage = rs.getBoolean("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    configs.add(config);
                }
                return configs;
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

    }

    @Override
    public List<SentinelConfig> getOrganizationConfigs(long organizationId, int limit, int offset)
            throws IotDatabaseException {
        String query = "SELECT * FROM sentinels WHERE organization_id=? ORDER BY id DESC LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, organizationId);
            pstmt.setInt(2, limit);
            pstmt.setInt(3, offset);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                java.util.ArrayList<SentinelConfig> configs = new java.util.ArrayList<>();
                while (rs.next()) {
                    SentinelConfig config = new SentinelConfig();
                    config.id = rs.getLong("id");
                    config.name = rs.getString("name");
                    config.active = rs.getBoolean("active");
                    config.userId = rs.getLong("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOkMessage = rs.getBoolean("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    configs.add(config);
                }
                return configs;
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public List<SentinelConfig> getConfigsByDevice(String deviceEui, int limit, int offset)
            throws IotDatabaseException {
        String query = "SELECT * FROM sentinels WHERE device_eui=? ORDER BY id DESC LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
            pstmt.setInt(2, limit);
            pstmt.setInt(3, offset);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                java.util.ArrayList<SentinelConfig> configs = new java.util.ArrayList<>();
                while (rs.next()) {
                    SentinelConfig config = new SentinelConfig();
                    config.id = rs.getLong("id");
                    config.name = rs.getString("name");
                    config.active = rs.getBoolean("active");
                    config.userId = rs.getLong("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOkMessage = rs.getBoolean("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    configs.add(config);
                }
                return configs;
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

}
