package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.SentinelDaoIface;
import com.signomix.common.iot.sentinel.SentinelConfig;

import io.agroal.api.AgroalDataSource;

public class SentinelDao implements SentinelDaoIface {

    public static final long DEFAULT_ORGANIZATION_ID = 1;

    Logger logger = Logger.getLogger(SentinelDao.class);

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
        logger.info("Creating sentinel tables...");
        String query = "CREATE TABLE IF NOT EXISTS sentinels ("
                + "id BIGSERIAL PRIMARY KEY,"
                + "tstamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + "name VARCHAR(255) NOT NULL,"
                + "active BOOLEAN NOT NULL,"
                + "user_id VARCHAR(255),"
                + "organization_id BIGINT NOT NULL,"
                + "type INTEGER NOT NULL,"
                + "device_eui VARCHAR(255),"
                + "group_eui VARCHAR(255),"
                + "tag_name VARCHAR(255),"
                + "tag_value VARCHAR(255),"
                + "alert_level INTEGER NOT NULL,"
                + "alert_message VARCHAR(255),"
                + "every_time BOOLEAN NOT NULL,"
                + "alert_ok BOOLEAN NOT NULL DEFAULT FALSE,"
                + "condition_ok_message TEXT NOT NULL DEFAULT '',"
                + "conditions JSON NOT NULL DEFAULT '[]',"
                + "team TEXT NOT NULL DEFAULT '',"
                + "administrators TEXT NOT NULL DEFAULT '',"
                + "time_shift INTEGER NOT NULL DEFAULT 1"
                + ");"
                + "CREATE TABLE IF NOT EXISTS sentinel_events ("
                + "id BIGSERIAL PRIMARY KEY,"
                + "device_eui VARCHAR(255) NOT NULL,"
                + "sentinel_id BIGINT NOT NULL,"
                + "tstamp TIMESTAMPTZ NOT NULL DEFAULT now(),"
                + "level INTEGER NOT NULL,"
                + "message_pl VARCHAR(255) NOT NULL DEFAULT '',"
                + "message_en VARCHAR(255) NOT NULL DEFAULT '',"
                + "propagated BOOLEAN NOT NULL DEFAULT FALSE"
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

        query = "SELECT create_hypertable('sentinel_events', 'tstamp');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }
    }

    @Override
    public long addConfig(SentinelConfig config) throws IotDatabaseException {
        String query = "INSERT INTO sentinels (name, active, user_id, organization_id, type, device_eui, group_eui, tag_name, tag_value, alert_level, alert_message, every_time,alert_ok, condition_ok_message, conditions, team, administrators, time_shift) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::json,?,?,?)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);) {
            pstmt.setString(1, config.name);
            pstmt.setBoolean(2, config.active);
            pstmt.setString(3, config.userId);
            pstmt.setLong(4, config.organizationId);
            pstmt.setInt(5, config.type);
            pstmt.setString(6, config.deviceEui);
            pstmt.setString(7, config.groupEui);
            pstmt.setString(8, config.tagName);
            pstmt.setString(9, config.tagValue);
            pstmt.setInt(10, config.alertLevel);
            pstmt.setString(11, config.alertMessage);
            pstmt.setBoolean(12, config.everyTime);
            pstmt.setBoolean(13, config.conditionOk);
            pstmt.setString(14, config.conditionOkMessage);
            pstmt.setObject(15, new ObjectMapper().writeValueAsString(config.conditions));
            pstmt.setString(16, config.team);
            pstmt.setString(17, config.administrators);
            pstmt.setInt(18, config.timeShift);
            pstmt.execute();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, "No generated key");
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void updateConfig(SentinelConfig config) throws IotDatabaseException {
        String query = "UPDATE sentinels SET name=?, active=?, user_id=?, organization_id=?, type=?, device_eui=?, group_eui=?, tag_name=?, tag_value=?, alert_level=?, alert_message=?, alert_ok=?, condition_ok_message=?, conditions=?::json, team=?, administrators=?, every_time=?, time_shift=? WHERE id=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, config.name);
            pstmt.setBoolean(2, config.active);
            pstmt.setString(3, config.userId);
            pstmt.setLong(4, config.organizationId);
            pstmt.setInt(5, config.type);
            pstmt.setString(6, config.deviceEui);
            pstmt.setString(7, config.groupEui);
            pstmt.setString(8, config.tagName);
            pstmt.setString(9, config.tagValue);
            pstmt.setInt(10, config.alertLevel);
            pstmt.setString(11, config.alertMessage);
            pstmt.setBoolean(12, config.conditionOk);
            pstmt.setString(13, config.conditionOkMessage);
            pstmt.setObject(14, new ObjectMapper().writeValueAsString(config.conditions));
            pstmt.setString(15, config.team);
            pstmt.setString(16, config.administrators);
            pstmt.setBoolean(17, config.everyTime);
            pstmt.setInt(18, config.timeShift);
            pstmt.setLong(19, config.id);
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
                    config.userId = rs.getString("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOk = rs.getBoolean("alert_ok");
                    config.conditionOkMessage = rs.getString("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    config.team = rs.getString("team");
                    config.administrators = rs.getString("administrators");
                    config.timeShift = rs.getInt("time_shift");
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
    public List<SentinelConfig> getConfigs(String userId, int limit, int offset) throws IotDatabaseException {
        String query = "SELECT * FROM sentinels WHERE user_id=? OR team LIKE ? OR administrators LIKE ? ORDER BY id DESC LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userId);
            pstmt.setString(2, "%," + userId + ",%");
            pstmt.setString(3, "%," + userId + ",%");
            pstmt.setInt(4, limit);
            pstmt.setInt(5, offset);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                java.util.ArrayList<SentinelConfig> configs = new java.util.ArrayList<>();
                while (rs.next()) {
                    SentinelConfig config = new SentinelConfig();
                    config.id = rs.getLong("id");
                    config.name = rs.getString("name");
                    config.active = rs.getBoolean("active");
                    config.userId = rs.getString("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOk = rs.getBoolean("alert_ok");
                    config.conditionOkMessage = rs.getString("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    config.team = rs.getString("team");
                    config.administrators = rs.getString("administrators");
                    config.timeShift = rs.getInt("time_shift");
                    configs.add(config);
                }
                return configs;
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
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
                    config.userId = rs.getString("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOk = rs.getBoolean("alert_ok");
                    config.conditionOkMessage = rs.getString("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    config.team = rs.getString("team");
                    config.administrators = rs.getString("administrators");
                    config.timeShift = rs.getInt("time_shift");
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
                    config.userId = rs.getString("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOk = rs.getBoolean("alert_ok");
                    config.conditionOkMessage = rs.getString("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    config.team = rs.getString("team");
                    config.administrators = rs.getString("administrators");
                    config.timeShift = rs.getInt("time_shift");
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
    public Map<String, Map<String, String>> getDeviceChannelsByConfigId(long configId)
            throws IotDatabaseException {
        String query = "SELECT eui,channels FROM sentinel_devices WHERE sentinel_id=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, configId);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                HashMap<String, String> channels = new HashMap<>();
                HashMap<String, Map<String, String>> devices = new HashMap<>();
                String channelsStr;
                while (rs.next()) {
                    channelsStr = rs.getString("channels");
                    String[] ch = channelsStr.split(";");
                    channels = new HashMap<>();
                    for (int i = 0; i < ch.length; i++) {
                        if (ch[i].isEmpty()) {
                            continue;
                        }
                        String[] ch2 = ch[i].split(":"); // exaple: temperature:d1
                        channels.put(ch2[0], ch2[1]); // exaple: temperature:d1
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

    /**
     * Returns last values for given devices.
     * Result is list of lists. Each list contains: deviceEui, timestamp, d1, d2,
     * d3, d4, d5, d6, d7, d8, d9, d10,
     * Example result: [ [ "eui1", "2020-01-01 00:00:00", 1.0, 2.0, 3.0 ], [ "eui2",    
     * * "2020-01-01 00:00:00", 4.0, 5.0, 6.0 ] ]
     * 
     * @param euis        set of device euis
     * @param secondsBack number of seconds to look back for data
     */
    @Override
    public List<List> getLastValuesOfDevices(Set<String> euis, long secondsBack) throws IotDatabaseException{
        String euiList = String.join(",", euis.stream().map(eui -> "'" + eui + "'").collect(Collectors.toList()));
        String query = "SELECT DISTINCT ON (eui) * FROM analyticdata "
                + "WHERE eui IN ("+euiList+") "
                + "AND tstamp > now() - INTERVAL '"+secondsBack+" seconds' ORDER BY eui, tstamp DESC;";
        logger.info(query);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
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
                e.printStackTrace();
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public List<List> getLastValuesByDeviceEui(String deviceEui) throws IotDatabaseException {
        String query = "SELECT DISTINCT ON (eui) * FROM analyticdata "
                + "WHERE eui=? "
                + "AND tstamp > now() - INTERVAL '15 minutes' ORDER BY eui, tstamp DESC;";

        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
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
    public void removeConfigDevice(long configId, String deviceEui) throws IotDatabaseException {
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
    public void removeDevice(String deviceEui) throws IotDatabaseException {
        String query = "DELETE FROM sentinel_devices WHERE eui=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void removeDevices(long configId) throws IotDatabaseException {
        String query = "DELETE FROM sentinel_devices WHERE sentinel_id=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, configId);
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

    @Override
    public void addSentinelEvent(long configId, String deviceEui, int level, String message_pl, String message_en)
            throws IotDatabaseException {
        String query = "INSERT INTO sentinel_events (sentinel_id, device_eui, level, message_pl, message_en) VALUES (?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, configId);
            pstmt.setString(2, deviceEui);
            pstmt.setInt(3, level);
            pstmt.setString(4, message_pl);
            pstmt.setString(5, message_en);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

    }

    @Override
    public int getSentinelStatus(long sentinelId) throws IotDatabaseException {
        String query = "SELECT DISTINCT ON (sentinel_id) level FROM sentinel_events WHERE sentinel_id=? AND tstamp > now() - INTERVAL '24 hours' ORDER BY sentinel_id, tstamp DESC;";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, sentinelId);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    return rs.getInt("level");
                }
            } catch (Exception e) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return 0;
    }

    @Override
    public List<SentinelConfig> getConfigsByTag(String tagName, String tagValue, int limit, int offset)
            throws IotDatabaseException {
        String query = "SELECT * FROM sentinels WHERE tag_name=? AND tag_value=? ORDER BY id DESC LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, tagName);
            pstmt.setString(2, tagValue);
            pstmt.setInt(3, limit);
            pstmt.setInt(4, offset);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                java.util.ArrayList<SentinelConfig> configs = new java.util.ArrayList<>();
                while (rs.next()) {
                    SentinelConfig config = new SentinelConfig();
                    config.id = rs.getLong("id");
                    config.name = rs.getString("name");
                    config.active = rs.getBoolean("active");
                    config.userId = rs.getString("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOk = rs.getBoolean("alert_ok");
                    config.conditionOkMessage = rs.getString("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    config.team = rs.getString("team");
                    config.administrators = rs.getString("administrators");
                    config.timeShift = rs.getInt("time_shift");
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
    public List<SentinelConfig> getConfigsByGroup(String groupName, int limit, int offset) throws IotDatabaseException {
        String query = "SELECT * FROM sentinels WHERE group_eui=? ORDER BY id DESC LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, groupName);
            pstmt.setInt(2, limit);
            pstmt.setInt(3, offset);
            try (java.sql.ResultSet rs = pstmt.executeQuery();) {
                java.util.ArrayList<SentinelConfig> configs = new java.util.ArrayList<>();
                while (rs.next()) {
                    SentinelConfig config = new SentinelConfig();
                    config.id = rs.getLong("id");
                    config.name = rs.getString("name");
                    config.active = rs.getBoolean("active");
                    config.userId = rs.getString("user_id");
                    config.organizationId = rs.getLong("organization_id");
                    config.type = rs.getInt("type");
                    config.deviceEui = rs.getString("device_eui");
                    config.groupEui = rs.getString("group_eui");
                    config.tagName = rs.getString("tag_name");
                    config.tagValue = rs.getString("tag_value");
                    config.alertLevel = rs.getInt("alert_level");
                    config.alertMessage = rs.getString("alert_message");
                    config.everyTime = rs.getBoolean("every_time");
                    config.conditionOk = rs.getBoolean("alert_ok");
                    config.conditionOkMessage = rs.getString("condition_ok_message");
                    config.conditions = new ObjectMapper().readValue(rs.getString("conditions"), List.class);
                    config.team = rs.getString("team");
                    config.administrators = rs.getString("administrators");
                    config.timeShift = rs.getInt("time_shift");
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
