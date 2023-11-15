package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.SentinelDaoIface;
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
                + "conditions JSON NOT NULL DEFAULT '[]',"
                + "team TEXT NOT NULL DEFAULT '',"
                + "administrators TEXT NOT NULL DEFAULT ''"
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
                + "channels TEXT NOT NULL"
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
        String query = "INSERT INTO sentinels (name, active, user_id, organization_id, type, device_eui, group, tag_name, tag_value, alert_level, alert_message, alert_ok, condition_ok_message, conditions, team, administrators) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?::json,?,?)";
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
            pstmt.setString(15, config.team);
            pstmt.setString(16, config.administrators);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void updateConfig(SentinelConfig config) throws IotDatabaseException {
        String query = "UPDATE sentinels SET name=?, active=?, user_id=?, organization_id=?, type=?, device_eui=?, group_eui=?, tag_name=?, tag_value=?, alert_level=?, alert_message=?, alert_ok=?, condition_ok_message=?, conditions=?::json, team=?, administrators=? WHERE id=?";
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
            pstmt.setString(15, config.team);
            pstmt.setString(16, config.administrators);
            pstmt.setLong(17, config.id);
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
                    config.team = rs.getString("team");
                    config.administrators = rs.getString("administrators");
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
        String query = "SELECT * FROM sentinels WHERE user_id=? OR team LIKE ? OR administrators LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, userId);
            pstmt.setString(2, "%,"+userId+",%");
            pstmt.setString(3, "%,"+userId+",%");
            pstmt.setInt(4, limit);
            pstmt.setInt(5, offset);
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
                    config.team=rs.getString("team");
                    config.administrators=rs.getString("administrators");
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
                    config.team=rs.getString("team");
                    config.administrators=rs.getString("administrators");
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
                    config.team=rs.getString("team");
                    config.administrators=rs.getString("administrators");
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

    /**
     * Returns map of devices and their channels for given sentinel config.
     * Example result: { "eui1": { "temperature": "d1", "humidity": "d2" }, "eui2":
     * { "temperature": "d14", "humidity": "d1" } }
     * 
     * @param configId sentinel config id
     * @param limit    query limit
     * @param offset   query offset
     */
    @Override
    public Map<String, Map<String, String>> getDevicesByConfigId(long configId, int limit, int offset)
            throws IotDatabaseException {
        String query = "SELECT eui,channels FROM sentinel_devices WHERE sentinel_id=? ORDER BY eui ASC LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, configId);
            pstmt.setInt(2, limit);
            pstmt.setInt(3, offset);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                HashMap<String, String> channels = new HashMap<>();
                HashMap<String, Map<String, String>> devices = new HashMap<>();
                String channelsStr;
                while (rs.next()) {
                    channelsStr = rs.getString("channels");
                    String[] ch = channelsStr.split(";");
                    channels = new HashMap<>();
                    for (int i = 0; i < ch.length; i++) {
                        if (ch[i].isEmpty())
                            continue;
                        String[] ch2 = ch[i].split(":"); // exaple: temperature:d1
                        channels.put(ch2[1], ch2[0]); // exaple: d1:temperature
                    }
                    devices.put(rs.getString("eui"), channels);
                }
                return devices;
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    /**
     * Returns last values for all devices related to given sentinel config.
     * Result is list of lists. Each list contains: deviceEui, timestamp, d1, d2,
     * d3, d4, d5, d6, d7, d8, d9, d10,
     * Example result: [ [ "eui1", "2020-01-01 00:00:00", 1.0, 2.0, 3.0 ], [ "eui2",
     * "2020-01-01 00:00:00", 4.0, 5.0, 6.0 ] ]
     * 
     * @param sentinelConfigId sentinel config id
     */
    @Override
    public List<List> getLastValuesByConfigId(long sentinelConfigId) throws IotDatabaseException {
        String query = "SELECT DISTINCT ON (eui) * FROM analyticdata "
                + "WHERE eui IN (SELECT eui FROM sentinel_devices WHERE sentinel_id=?) "
                + "AND tstamp > now() - INTERVAL '24 hours' ORDER BY eui, tstamp DESC;";

        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, sentinelConfigId);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                java.util.ArrayList<List> values = new java.util.ArrayList<>();
                while (rs.next()) {
                    java.util.ArrayList<Object> row = new java.util.ArrayList<>();
                    row.add(rs.getString("eui"));
                    row.add(rs.getTimestamp("tstamp"));
                    row.add(rs.getDouble("d1"));
                    row.add(rs.getDouble("d2"));
                    row.add(rs.getDouble("d3"));
                    row.add(rs.getDouble("d4"));
                    row.add(rs.getDouble("d5"));
                    row.add(rs.getDouble("d6"));
                    row.add(rs.getDouble("d7"));
                    row.add(rs.getDouble("d8"));
                    row.add(rs.getDouble("d9"));
                    row.add(rs.getDouble("d10"));
                    row.add(rs.getDouble("d11"));
                    row.add(rs.getDouble("d12"));
                    row.add(rs.getDouble("d13"));
                    row.add(rs.getDouble("d14"));
                    row.add(rs.getDouble("d15"));
                    row.add(rs.getDouble("d16"));
                    row.add(rs.getDouble("d17"));
                    row.add(rs.getDouble("d18"));
                    row.add(rs.getDouble("d19"));
                    row.add(rs.getDouble("d20"));
                    row.add(rs.getDouble("d21"));
                    row.add(rs.getDouble("d22"));
                    row.add(rs.getDouble("d23"));
                    row.add(rs.getDouble("d24"));
                    values.add(row);
                }
                return values;
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void addDevice(long configId, String deviceEui, String channelMapping) throws IotDatabaseException {
        String query = "INSERT INTO sentinel_devices (sentinel_id, eui, channels) VALUES (?,?,?)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, configId);
            pstmt.setString(2, deviceEui);
            pstmt.setString(3, channelMapping);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void removeDevice(long configId, String deviceEui) throws IotDatabaseException {
        String query = "DELETE FROM sentinel_devices WHERE sentinel_id=? AND eui=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, configId);
            pstmt.setString(2, deviceEui);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public Map<String, String> getDevices(long configId) throws IotDatabaseException {
        String query = "SELECT eui,channels FROM sentinel_devices WHERE sentinel_id=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, configId);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                HashMap<String, String> devices = new HashMap<>();
                while (rs.next()) {
                    devices.put(rs.getString("eui"), rs.getString("channels"));
                }
                return devices;
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

}
