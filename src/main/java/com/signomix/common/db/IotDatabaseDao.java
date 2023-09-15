package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import javax.inject.Singleton;

import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

import com.cedarsoftware.util.io.JsonObject;
import com.cedarsoftware.util.io.JsonReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.signomix.common.User;
import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.Alert;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.virtual.VirtualData;

import io.agroal.api.AgroalDataSource;
import io.quarkus.cache.CacheResult;

@Singleton
public class IotDatabaseDao implements IotDatabaseIface {
    private static final Logger LOG = Logger.getLogger(IotDatabaseDao.class);

    Long defaultOrganizationId = ConfigProvider.getConfig().getValue("signomix.default.organization.id", Long.class);
    Long defaultApplicationId = ConfigProvider.getConfig().getValue("signomix.default.application.id", Long.class);
    private AgroalDataSource dataSource;

    // TODO: get requestLimit from config
    private long requestLimit = 500;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void setQueryResultsLimit(int limit) {
        requestLimit = limit;
        LOG.info("requestLimit:" + requestLimit);
    }

    @Override
    public ChannelData getLastValue(String userID, String deviceEUI, String channel) throws IotDatabaseException {
        int channelIndex = getChannelIndex(deviceEUI, channel);
        if (channelIndex < 0) {
            return null;
        }
        String columnName = "d" + (channelIndex);
        String query = "select eui,userid,day,dtime,tstamp," + columnName
                + " from devicedata where eui=? order by tstamp desc limit 1";
        ChannelData result = null;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            Double d;
            if (rs.next()) {
                d = rs.getDouble(6);
                if (!rs.wasNull()) {
                    result = new ChannelData(deviceEUI, channel, d, rs.getTimestamp(5).getTime());
                }
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public List<List<List>> getValuesOfGroup(String userID, long organizationId, String groupEUI, String channelNames,
            long secondsBack)
            throws IotDatabaseException {
        String[] channels = channelNames.split(",");
        return getGroupLastValues(userID, organizationId, groupEUI, channels, secondsBack);
    }

    public List<String> getGroupChannels(String groupEUI) throws IotDatabaseException {
        List<String> channels;
        // return ((Service) Kernel.getInstance()).getDataStorageAdapter().
        String query = "select channels from groups where eui=?";
        channels = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, groupEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                String[] s = rs.getString(1).toLowerCase().split(",");
                channels = Arrays.asList(s);
            }
            return channels;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    public List<List<List>> getGroupLastValues(String userID, long organizationID, String groupEUI,
            String[] channelNames, long secondsBack)
            throws IotDatabaseException {
        List<String> requestChannels = Arrays.asList(channelNames);
        try {
            String group = "%," + groupEUI + ",%";
            long timestamp = System.currentTimeMillis() - secondsBack * 1000;
            String deviceQuery = "SELECT eui,channels FROM devices WHERE groups like ?;";
            HashMap<String, List> devices = new HashMap<>();
            String query;
            query = "SELECT "
                    + "eui,userid,day,dtime,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24 "
                    + "FROM devicedata "
                    + "WHERE eui IN "
                    + "(SELECT eui FROM devices WHERE groups like ?) "
                    + "and (tstamp>?) "
                    + "order by eui,tstamp desc;";
            List<String> groupChannels = getGroupChannels(groupEUI);
            if (requestChannels.size() == 0) {
                // logger.error("empty channelNames");
                requestChannels = groupChannels;
            }
            List<List<List>> result = new ArrayList<>();
            List<List> measuresForEui = new ArrayList<>();
            List<ChannelData> measuresForEuiTimestamp = new ArrayList<>();
            List<ChannelData> tmpResult = new ArrayList<>();
            ChannelData cd;
            // logger.debug("{} {} {} {} {}", groupEUI, group, groupChannels.size(),
            // requestChannels.size(), query);
            // .info("query withseconds back: " + query);
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstd = conn.prepareStatement(deviceQuery);
                    PreparedStatement pst = conn.prepareStatement(query);) {
                pstd.setString(1, group);
                ResultSet rs = pstd.executeQuery();
                while (rs.next()) {
                    devices.put(rs.getString(1), Arrays.asList(rs.getString(2).split(",")));
                }
                pst.setString(1, group);
                pst.setTimestamp(2, new Timestamp(timestamp));
                rs = pst.executeQuery();
                int channelIndex;
                String channelName;
                String devEui;
                double d;
                while (rs.next()) {
                    for (int i = 0; i < groupChannels.size(); i++) {
                        devEui = rs.getString(1);
                        channelName = groupChannels.get(i);
                        channelIndex = devices.get(devEui).indexOf(channelName);
                        d = rs.getDouble(6 + channelIndex);
                        if (!rs.wasNull()) {
                            tmpResult.add(new ChannelData(devEui, channelName, d,
                                    rs.getTimestamp(5).getTime()));
                        }
                    }
                }
            } catch (SQLException e) {
                // logger.error(e.getMessage());
                throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
            } catch (Exception ex) {
                // logger.error(ex.getMessage());
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN, ex.getMessage());
            }
            if (tmpResult.isEmpty()) {
                return result;
            }
            long processedTimestamp = 0;
            String prevEUI = "";
            long prevTimestamp = 0;
            int idx;
            for (int i = 0; i < tmpResult.size(); i++) {
                cd = tmpResult.get(i);
                // logger.info("ChannelData: {} {} {}", cd.getDeviceEUI(), cd.getName(),
                // cd.getTimestamp());
                if (!cd.getDeviceEUI().equalsIgnoreCase(prevEUI)) {
                    if (!measuresForEuiTimestamp.isEmpty()) {
                        measuresForEui.add(measuresForEuiTimestamp);
                    }
                    if (!measuresForEui.isEmpty()) {
                        result.add(measuresForEui);
                    }
                    measuresForEui = new ArrayList<>();
                    measuresForEuiTimestamp = new ArrayList<>();
                    for (int j = 0; j < requestChannels.size(); j++) {
                        measuresForEuiTimestamp.add(null);
                    }
                    idx = requestChannels.indexOf(cd.getName());
                    if (idx > -1) {
                        measuresForEuiTimestamp.set(idx, cd);
                    }
                    prevEUI = cd.getDeviceEUI();
                    prevTimestamp = cd.getTimestamp();
                } else {
                    if (prevTimestamp == cd.getTimestamp()) {
                        // next measurement
                        idx = requestChannels.indexOf(cd.getName());
                        if (idx > -1) {
                            measuresForEuiTimestamp.set(idx, cd);
                        }
                    } else {
                        // skip prevous measures
                    }
                }
            }
            if (!measuresForEuiTimestamp.isEmpty()) {
                measuresForEui.add(measuresForEuiTimestamp);
            }
            if (!measuresForEui.isEmpty()) {
                result.add(measuresForEui);
            }
            return result;
        } catch (Exception e) {
            StackTraceElement[] ste = e.getStackTrace();
            // logger.error("requestChannels[{}]", requestChannels.size());
            // logger.error("channelNames[{}]", channelNames.length);
            // logger.error(e.getMessage());
            for (int i = 0; i < ste.length; i++) {
                // logger.error("{}.{}:{}", e.getStackTrace()[i].getClassName(),
                // e.getStackTrace()[i].getMethodName(),
                // e.getStackTrace()[i].getLineNumber());
            }
            return null;
        }
    }

    @Override
    public IotEvent getFirstCommand(String deviceEUI) throws IotDatabaseException {
        String query = "select id,category,type,origin,payload,createdat from commands where origin like ? order by createdat limit 1";
        IotEvent result = null;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, "%@" + deviceEUI);
            // pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                // result = new IotEvent(deviceEUI, rs.getString(2), rs.getString(3), null,
                // rs.getString(4));
                result = new IotEvent();
                result.setId(rs.getLong(1));
                result.setCategory(rs.getString(2));
                result.setType(rs.getString(3));
                result.setOrigin(rs.getString(4));
                result.setPayload(rs.getString(5));
                result.setCreatedAt(rs.getLong(6));
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public long getMaxCommandId() throws IotDatabaseException {
        String query = "SELECT  max(commands.id), max(commandslog.id) FROM commands CROSS JOIN commandslog";
        long result = 0;
        long v1 = 0;
        long v2 = 0;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                v1 = rs.getLong(1);
                v1 = rs.getLong(2);
            }
            if (v1 > v2) {
                result = v1;
            } else {
                result = v2;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION);
        }
        return result;
    }

    @Override
    public long getMaxCommandId(String deviceEui) throws IotDatabaseException {
        String query = "SELECT  max(commands.id), max(commandslog.id) FROM commands CROSS JOIN commandslog "
                + "WHERE commands.origin=commandslog.origin AND commands.origin like %@" + deviceEui;
        long result = 0;
        long v1 = 0;
        long v2 = 0;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                v1 = rs.getLong(1);
                v1 = rs.getLong(2);
            }
            if (v1 > v2) {
                result = v1;
            } else {
                result = v2;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION);
        }
        return result;
    }

    @Override
    public void removeCommand(long id) throws IotDatabaseException {
        String query = "delete from commands where id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, id);
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void putVirtualData(Device device, VirtualData data) throws IotDatabaseException {
        JsonMapper mapper = new JsonMapper();
        String serialized;
        try {
            serialized = mapper.writeValueAsString(data);
            LOG.debug(serialized);
        } catch (JsonProcessingException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, "", null);
        }
        String query = "MERGE INTO virtualdevicedata (eui, tstamp, data) KEY (eui) values (?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, device.getEUI());
            pst.setTimestamp(2, new Timestamp(data.timestamp));
            pst.setString(3, serialized);
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    @CacheResult(cacheName = "devchannels-cache")
    public int getChannelIndex(String deviceEUI, String channel) throws IotDatabaseException {
        return getDeviceChannels(deviceEUI).indexOf(channel) + 1;
    }

    @Override
    public void putCommandLog(String deviceEUI, IotEvent commandEvent) throws IotDatabaseException {
        String query = "insert into commandslog (id,category,type,origin,payload,createdat) values (?,?,?,?,?,?);";
        String command = (String) commandEvent.getPayload();
        if (command.startsWith("#") || command.startsWith("&")) {
            command = command.substring(1);
        }
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, commandEvent.getId());
            pst.setString(2, commandEvent.getCategory());
            pst.setString(3, commandEvent.getType());
            pst.setString(4, deviceEUI);
            pst.setString(5, command);
            pst.setLong(6, commandEvent.getCreatedAt());
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void addAlert(IotEvent event) throws IotDatabaseException {
        Alert alert = new Alert(event);
        String query = "insert into alerts (name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, alert.getName());
            pstmt.setString(2, alert.getCategory());
            pstmt.setString(3, alert.getType());
            pstmt.setString(4, alert.getDeviceEUI());
            pstmt.setString(5, alert.getUserID());
            pstmt.setString(6, (null != alert.getPayload()) ? alert.getPayload().toString() : "");
            pstmt.setString(7, "");
            pstmt.setString(8, "");
            pstmt.setString(9, "");
            pstmt.setLong(10, 0);
            pstmt.setLong(11, alert.getCreatedAt());
            pstmt.setLong(12, -1);
            pstmt.setBoolean(13, false);
            int updated = pstmt.executeUpdate();
            if (updated < 1) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN,
                        "Unable to create notification " + alert.getId(), null);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage(), null);
        }
    }

    @Override
    public List<Alert> getAlerts(String userID, boolean descending) throws IotDatabaseException {
        String query = "select id,name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic from alerts where userid = ? order by id ";
        if (descending) {
            query = query.concat(" desc");
        }
        query = query.concat(" limit ?");
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setLong(2, requestLimit);
            ResultSet rs = pstmt.executeQuery();
            ArrayList<Alert> list = new ArrayList<>();
            while (rs.next()) {
                list.add(buildAlert(rs));
            }
            return list;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public Long getAlertsCount(String userID) throws IotDatabaseException {
        Long result = 0L;
        String query = "select count(*) from alerts where userid = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                result = rs.getLong(1);
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public List<Alert> getAlerts(String userID, int limit, int offset, boolean descending) throws IotDatabaseException {
        ArrayList<Alert> list = new ArrayList<>();
        String query = "select id,name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic from alerts where userid = ? order by id ";
        if (descending) {
            query = query.concat(" desc");
        }
        query = query.concat(" limit ? offset ?");
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setLong(2, limit);
            pstmt.setLong(3, offset);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                list.add(buildAlert(rs));
            }
            return list;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void removeAlert(long alertID) throws IotDatabaseException {
        String query = "delete from alerts where id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, alertID);
            int updated = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeAlerts(String userID) throws IotDatabaseException {
        String query = "delete from alerts where userid=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            int updated = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeAlerts(String userID, long checkpoint) throws IotDatabaseException {
        String query = "delete from alerts where userid=? and createdat < ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setLong(2, checkpoint);
            int updated = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeOutdatedAlerts(long checkpoint) throws IotDatabaseException {
        String query = "delete from alerts where createdat < ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, checkpoint);
            int updated = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Alert buildAlert(ResultSet rs) throws SQLException {
        // id,name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic
        // Device d = new Device();
        Alert a = new Alert();
        a.setId(rs.getLong(1));
        a.setName(rs.getString(2));
        a.setCategory(rs.getString(3));
        a.setType(rs.getString(4));
        a.setOrigin(rs.getString(6) + "\t" + rs.getString(5));
        a.setPayload(rs.getString(7));
        a.setCreatedAt(rs.getLong(12));
        return a;
    }

    private String buildDeviceQuery() {
        String query = "SELECT"
                + " d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder, d.devicekey, d.description, d.lastseen, d.tinterval,"
                + " d.lastframe, d.template, d.pattern, d.downlink, d.commandscript, d.appid, d.groups, d.alert,"
                + " d.appeui, d.devid, d.active, d.project, d.latitude, d.longitude, d.altitude, d.state, d.retention,"
                + " d.administrators, d.framecheck, d.configuration, d.organization, d.organizationapp, a.configuration FROM devices AS d"
                + " LEFT JOIN applications AS a WHERE d.organizationapp=a.id";
        return query;
    }

    @Override
    public void addSmsLog(long id, boolean confirmed, String phone, String text) throws IotDatabaseException {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeOutdatedSmsLogs(long checkpoint) throws IotDatabaseException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setConfirmedSms(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub

    }

    @Override
    public List<Long> getUnconfirmedSms() throws IotDatabaseException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void removeOldData() throws IotDatabaseException {
        // TODO: remove old data
        /*
         * String query = "delete from devicedata where eui=? and tstamp<?";
         * try (Connection conn = getConnection(); PreparedStatement pst =
         * conn.prepareStatement(query);) {
         * pst.setString(1, deviceEUI);
         * pst.setTimestamp(2, new java.sql.Timestamp(checkPoint));
         * pst.executeUpdate();
         * } catch (SQLException e) {
         * throw new ThingsDataException(ThingsDataException.BAD_REQUEST,
         * e.getMessage());
         * }
         * String query = "delete from commands where origin like ? and createdat<?";
         * try (Connection conn = getConnection(); PreparedStatement pst =
         * conn.prepareStatement(query);) {
         * pst.setString(1, "%@" + deviceEUI);
         * pst.setLong(2, checkPoint);
         * // pst.setTimestamp(2, new java.sql.Timestamp(checkPoint));
         * pst.executeUpdate();
         * } catch (SQLException e) {
         * throw new ThingsDataException(ThingsDataException.BAD_REQUEST,
         * e.getMessage());
         * }
         * String query = "delete from commandslog where origin like ? and createdat<?";
         * try (Connection conn = getConnection(); PreparedStatement pst =
         * conn.prepareStatement(query);) {
         * pst.setString(1, "%@" + deviceEUI);
         * pst.setLong(2, checkPoint);
         * // pst.setTimestamp(2, new java.sql.Timestamp(checkPoint));
         * pst.executeUpdate();
         * } catch (SQLException e) {
         * throw new ThingsDataException(ThingsDataException.BAD_REQUEST,
         * e.getMessage());
         * }
         * String query = "delete from alerts where userid=? and createdat < ?";
         * try (Connection conn = getConnection(); PreparedStatement pstmt =
         * conn.prepareStatement(query);) {
         * pstmt.setString(1, userID);
         * pstmt.setLong(2, checkpoint);
         * int updated = pstmt.executeUpdate();
         * } catch (SQLException e) {
         * throw new ThingsDataException(ThingsDataException.HELPER_EXCEPTION,
         * e.getMessage());
         * } catch (Exception e) {
         * e.printStackTrace();
         * }
         * String query = "delete from devicestatus where userid=? and createdat < ?";
         * try (Connection conn = getConnection(); PreparedStatement pstmt =
         * conn.prepareStatement(query);) {
         * pstmt.setString(1, userID);
         * pstmt.setLong(2, checkpoint);
         * int updated = pstmt.executeUpdate();
         * } catch (SQLException e) {
         * throw new ThingsDataException(ThingsDataException.HELPER_EXCEPTION,
         * e.getMessage());
         * } catch (Exception e) {
         * e.printStackTrace();
         * }
         */
    }

    @Override
    public void updateDeviceStatus(String eui, long transmissionInterval, Double newStatus, int newAlertStatus)
            throws IotDatabaseException {
        LOG.debug("Updating device status.");
        /*
         * Device device = getDevice(eui);
         * if (device == null) {
         * LOG.warn("Device " + eui + " not found");
         * throw new IotDatabaseException(IotDatabaseException.NOT_FOUND,
         * "device not found", null);
         * }
         * device.setState(newStatus);
         */
        // Device previous = getDevice(device.getEUI());
        String query;
        // if (null != newStatus) {
        query = "INSERT INTO devicestatus (eui, tinterval, status, alert) VALUES (?, ?, ?, ?)";
        // } else {
        // query = "update devices set lastseen=?,lastframe=?,downlink=?,devid=? where
        // eui=?";
        // }
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, eui);
            pstmt.setLong(2, transmissionInterval);
            pstmt.setDouble(3, newStatus);
            pstmt.setInt(4, newAlertStatus);
            int updated = pstmt.executeUpdate();
            if (updated < 1) {
                LOG.warn("DB error updating device " + eui);
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN,
                        "DB error updating device " + eui, null);
            } else {
                LOG.debug("Status rows updated: " + updated);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage(), null);
        }
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "CALL CSVWRITE('backup/alerts.csv', 'SELECT * FROM alerts');"
                + "CALL CSVWRITE('backup/applications.csv', 'SELECT * FROM applications');"
                + "CALL CSVWRITE('backup/commands.csv', 'SELECT * FROM commands');"
                + "CALL CSVWRITE('backup/commandslog.csv', 'SELECT * FROM commandslog');"
                + "CALL CSVWRITE('backup/dashboards.csv', 'SELECT * FROM dashboards');"
                + "CALL CSVWRITE('backup/dashboardtemplates.csv', 'SELECT * FROM dashboardtemplates');"
                + "CALL CSVWRITE('backup/devicechannels.csv', 'SELECT * FROM devicechannels');"
                + "CALL CSVWRITE('backup/devicedata.csv', 'SELECT * FROM devicedata');"
                + "CALL CSVWRITE('backup/devicestatus.csv', 'SELECT * FROM devicestatus');"
                + "CALL CSVWRITE('backup/devices.csv', 'SELECT * FROM devices');"
                + "CALL CSVWRITE('backup/devicetemplates.csv', 'SELECT * FROM devicetemplates');"
                + "CALL CSVWRITE('backup/groups.csv', 'SELECT * FROM groups');"
                + "CALL CSVWRITE('backup/virtualdevicedata.csv', 'SELECT * FROM virtualdevicedata');"
                + "CALL CSVWRITE('backup/devicestatus.csv', 'SELECT * FROM devicestatus');"
                + "CALL CSVWRITE('backup/account_params.csv', 'SELECT * FROM account_params');"
                + "CALL CSVWRITE('backup/account_features.csv', 'SELECT * FROM account_features');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void putDeviceCommand(String deviceEUI, IotEvent commandEvent) throws IotDatabaseException {
        String query = "insert into commands (id,category,type,origin,payload,createdat) values (?,?,?,?,?,?);";
        String query2 = "merge into commands (id,category,type,origin,payload,createdat) key (id) values (?,?,?,?,?,?)";
        String command = (String) commandEvent.getPayload();
        boolean overwriteMode = false;
        if (command.startsWith("&")) {
            overwriteMode = false;
        } else if (command.startsWith("#")) {
            query = query2;
            overwriteMode = true;
        }
        command = command.substring(1);
        String origin = commandEvent.getOrigin();
        if (null == origin || origin.isEmpty()) {
            origin = deviceEUI;
        }
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, commandEvent.getId());
            pst.setString(2, commandEvent.getCategory());
            pst.setString(3, commandEvent.getType());
            pst.setString(4, origin);
            pst.setString(5, command);
            pst.setLong(6, commandEvent.getCreatedAt());
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

    }

    @Override
    public void putData(Device device, ArrayList<ChannelData> values) throws IotDatabaseException {
        if (values == null || values.isEmpty()) {
            System.out.println("no values");
            return;
        }
        int limit = 24;
        List channelNames = getDeviceChannels(device.getEUI());
        String query = "insert into devicedata (eui,userid,day,dtime,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,project,state) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        long timestamp = values.get(0).getTimestamp();
        java.sql.Date date = new java.sql.Date(timestamp);
        java.sql.Time time = new java.sql.Time(timestamp);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, device.getEUI());
            pst.setString(2, device.getUserID());
            pst.setDate(3, date);
            pst.setTime(4, time);
            pst.setTimestamp(5, new java.sql.Timestamp(timestamp));
            for (int i = 1; i <= limit; i++) {
                pst.setNull(i + 5, java.sql.Types.DOUBLE);
            }
            int index = -1;
            // if (values.size() <= limit) {
            // limit = values.size();
            // }
            if (values.size() > limit) {
                // TODO: send notification to the user?
            }
            for (int i = 1; i <= limit; i++) {
                if (i <= values.size()) {
                    index = channelNames.indexOf(values.get(i - 1).getName());
                    if (index >= 0 && index < limit) { // TODO: there must be control of mthe number of measures while
                        // defining device, not here
                        try {
                            pst.setDouble(6 + index, values.get(i - 1).getValue());
                        } catch (NullPointerException e) {
                            pst.setNull(6 + index, Types.DOUBLE);
                        }
                    }
                }
            }
            pst.setString(30, device.getProject());
            pst.setDouble(31, device.getState());
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

    }

    /*
     * @Override
     * public Device getDevice(User user, String deviceEUI, boolean withShared,
     * boolean withStatus)
     * throws IotDatabaseException {
     * String query;
     * if (withShared) {
     * query = buildDeviceQuery()
     * +
     * " AND (upper(d.eui)=upper(?) AND (d.userid = ? OR d.team like ? OR d.administrators like ?))"
     * ;
     * } else {
     * query = buildDeviceQuery() +
     * " AND ( upper(d.eui)=upper(?) and d.userid = ?)";
     * }
     * LOG.info("getDevice query: " + query);
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt =
     * conn.prepareStatement(query);) {
     * pstmt.setString(1, deviceEUI);
     * pstmt.setString(2, userID);
     * if (withShared) {
     * pstmt.setString(3, "%," + userID + ",%");
     * pstmt.setString(4, "%," + userID + ",%");
     * }
     * ResultSet rs = pstmt.executeQuery();
     * if (rs.next()) {
     * Device device = buildDevice(rs);
     * if (withStatus) {
     * device = getDeviceStatusData(device);
     * }
     * return device;
     * } else {
     * return null;
     * }
     * } catch (SQLException e) {
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
     * e.getMessage(), e);
     * }
     * }
     */

    @CacheResult(cacheName = "device-cache")
    @Override
    public Device getDevice(String deviceEUI, boolean withStatus) throws IotDatabaseException {
        if (deviceEUI == null || deviceEUI.isEmpty()) {
            return null;
        }
        DeviceSelector selector = new DeviceSelector(null, false, withStatus, true, null, null);
        String query = selector.query;
        // String query = buildDeviceQuery() + " AND ( upper(d.eui) = upper(?))";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Device device = buildDevice(rs);
                if (withStatus) {
                    device = getDeviceStatusData(device);
                }
                return device;
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    private Device getDeviceStatusData(Device device) throws IotDatabaseException {
        String query = "SELECT ts,status,alert,tinterval FROM devicestatus WHERE eui=? ORDER BY ts DESC LIMIT 1";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, device.getEUI());
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                device.setLastSeen(rs.getTimestamp("ts").getTime());
                device.setState(rs.getDouble("status"));
                device.setAlertStatus(rs.getInt("alert"));
                // device.setTransmissionInterval(rs.getLong("tinterval"));
            }
            return device;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @CacheResult(cacheName = "devchannels-cache1")
    public LinkedHashMap<String, Integer> getDeviceChannelPositions(String deviceEUI) throws IotDatabaseException {
        LinkedHashMap<String, Integer> channels = new LinkedHashMap<String, Integer>();
        String query = "select channels from devicechannels where eui=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                String[] ch = rs.getString(1).toLowerCase().split(",");
                for (int i = 0; i < ch.length; i++) {
                    channels.put(ch[i], i + 1);
                }
            }
            return channels;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public List<String> getDeviceChannels(String deviceEUI) throws IotDatabaseException {
        List<String> channels;
        String query = "select channels from devicechannels where eui=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                String[] s = rs.getString(1).toLowerCase().split(",");
                channels = Arrays.asList(s);
                String channelStr = "";
                for (int i = 0; i < channels.size(); i++) {
                    channelStr = channelStr + channels.get(i) + ",";
                }
                LOG.debug("CHANNELS READ: " + deviceEUI + " " + channelStr);
                return channels;
            } else {
                return new ArrayList<>();
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    @CacheResult(cacheName = "values-cache2")
    public List<List> getValues2(String userID, String deviceEUI, String dataQuery) throws IotDatabaseException {
        DataQuery dq;
        try {
            dq = DataQuery.parse(dataQuery);
            if (dq.getChannels().size() == 1 && dq.getChannels().get(0).equals("*")) {
                dq.setChannels(getDeviceChannels(deviceEUI));
            }
        } catch (DataQueryException ex) {
            throw new IotDatabaseException(ex.getCode(), "DataQuery " + ex.getMessage());
        }
        if (dq.isVirtual()) {
            return getVirtualDeviceMeasures(userID, deviceEUI, dq); // TODO: refactor
        }
        LinkedHashMap<String, Integer> columnPositions = getDeviceChannelPositions(deviceEUI);
        ArrayList<String> columnSymbols = getColumnSymbols(dq, columnPositions);
        String query = buildDataQuery(userID, deviceEUI, dq, columnSymbols);
        int limit = dq.getLimit();
        if (dq.average > 0) {
            limit = dq.average;
        }
        if (dq.minimum > 0) {
            limit = dq.minimum;
        }
        if (dq.maximum > 0) {
            limit = dq.maximum;
        }
        if (dq.summary > 0) {
            limit = dq.summary;
        }
        if (dq.getNewValue() != null) {
            limit = limit - 1;
        }
        ArrayList<List> values = new ArrayList<>();
        int idx = 1;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {

            pstmt.setString(1, deviceEUI);
            idx = 2;
            if (null != dq.getProject()) {
                pstmt.setString(idx, dq.getProject());
                idx++;
            }
            if (null != dq.getState()) {
                pstmt.setDouble(idx, dq.getState());
                idx++;
            }
            if (null != dq.getFromTs() && null != dq.getToTs()) {
                pstmt.setTimestamp(idx, dq.getFromTs());
                idx++;
                pstmt.setTimestamp(idx, dq.getToTs());
                idx++;
            }
            pstmt.setInt(idx, limit);

            ResultSet rs = pstmt.executeQuery();
            List<ChannelData> row;
            ChannelData channelData;
            String eui;
            // String userid;
            // String project;
            long tstamp;
            // Double status;
            // String channelName;
            // Double value;
            while (rs.next()) {
                row = new ArrayList<>();
                eui = rs.getString("eui");
                // userid=rs.getString("userid");
                tstamp = rs.getTimestamp("tstamp").getTime();
                // status=rs.getDouble("state"); //not used
                // project=rs.getString("project"); //not used
                int i = 0;
                for (String columnSymbol : columnSymbols) {
                    channelData = new ChannelData();
                    channelData.setDeviceEUI(eui);
                    channelData.setName(dq.getChannels().get(i));
                    channelData.setTimestamp(tstamp);
                    channelData.setValue(rs.getDouble(columnSymbol));
                    if (rs.wasNull()) {
                        channelData.setNullValue();
                    }
                    row.add(channelData);
                    i++;
                }
                values.add(row);
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        Collections.reverse(values);
        return values;
    }

    private ArrayList<String> getColumnSymbols(DataQuery dq, HashMap<String, Integer> columnPositions) {
        ArrayList<String> columnSymbols = new ArrayList<>();
        Integer columnPosition;
        if ("*".equals(dq.getChannelName())) {
            for (String key : columnPositions.keySet()) {
                columnSymbols.add("d" + columnPositions.get(key));
            }

        } else {
            for (String column : dq.getChannels()) {
                columnPosition = columnPositions.get(column);
                if (columnPosition != null) {
                    columnSymbols.add("d" + columnPosition);
                }
            }
        }
        return columnSymbols;
    }

    private String buildDataQuery(String userID, String deviceEUI, DataQuery dq, ArrayList<String> columnSymbols) {

        String query = "SELECT eui,userid,tstamp,project,state ";
        for (String columnName : columnSymbols) {
            query = query + "," + columnName;
        }
        query = query + " FROM devicedata WHERE eui=?";

        // if there is only one column, we need to add IS NOT NULL to the query
        if (columnSymbols.size() == 1) {
            query = query + " AND " + columnSymbols.get(0) + " IS NOT NULL";
        }
        String projectQuery = " AND project=?";
        String statusQuery = " AND state=?";
        String wherePart = " AND tstamp BETWEEN ? AND ?";
        String orderPart = " ORDER BY tstamp DESC LIMIT ?";

        if (null != dq.getProject()) {
            query = query.concat(projectQuery);
        }
        if (null != dq.getState()) {
            query = query.concat(statusQuery);
        }
        LOG.debug("fromTs:" + dq.getFromTs());
        LOG.debug("toTs:" + dq.getToTs());
        if (null != dq.getFromTs() && null != dq.getToTs()) {
            query = query.concat(wherePart);
        }
        query = query.concat(orderPart);
        return query;
    }

    @Override
    @CacheResult(cacheName = "values-cache")
    public List<List> getValues(String userID, String deviceEUI, String dataQuery)
            throws IotDatabaseException {
        LOG.debug("queryLimit:" + requestLimit);
        LOG.debug("getValues dataQuery:" + dataQuery);
        DataQuery dq;
        try {
            dq = DataQuery.parse(dataQuery);
        } catch (DataQueryException ex) {
            throw new IotDatabaseException(ex.getCode(), "DataQuery " + ex.getMessage());
        }
        if (dq.isVirtual()) {
            return getVirtualDeviceMeasures(userID, deviceEUI, dq);
        }
        if (null != dq.getGroup()) {
            String channelName = dq.getChannelName();
            if (null == channelName) {
                channelName = "";
            }
            // return getValuesOfGroup(userID, dq.getGroup(), channelName.split(","),
            // defaultGroupInterval, dq);
            return new ArrayList<>();
        }

        int limit = dq.getLimit();
        if (dq.average > 0) {
            limit = dq.average;
        }
        if (dq.minimum > 0) {
            limit = dq.minimum;
        }
        if (dq.maximum > 0) {
            limit = dq.maximum;
        }
        if (dq.summary > 0) {
            limit = dq.summary;
        }
        List<List> result = new ArrayList<>();
        if (dq.getNewValue() != null) {
            limit = limit - 1;
        }

        if (null == dq.getChannelName() || "*".equals(dq.getChannelName())) {
            // TODO
            result.add(getValues(userID, deviceEUI, limit, dq));
            return result;
        }
        boolean singleChannel = !dq.getChannelName().contains(",");
        long t0, t1, t2;
        t0 = System.currentTimeMillis();
        if (singleChannel) {
            result.add(getChannelValues(userID, deviceEUI, dq.getChannelName(), limit, dq)); // project
            t1 = System.currentTimeMillis();
            LOG.debug("Query time [ms] 1: " + (t1 - t0));
        } else {
            String[] channels = dq.getChannelName().split(",");
            List<ChannelData>[] temp = new ArrayList[channels.length];
            for (int i = 0; i < channels.length; i++) {
                temp[i] = getChannelValues(userID, deviceEUI, channels[i], limit, dq); // project
            }
            t1 = System.currentTimeMillis();
            LOG.debug("Query time [ms] 2: " + (t1 - t0));
            List<ChannelData> values;
            LOG.debug("DQ limit: " + limit);
            int realLimit = 0;
            for (int j = 0; j < channels.length; j++) {
                if (temp[j].size() > realLimit) {
                    realLimit = temp[j].size();
                }
            }
            LOG.debug("Result limit: " + realLimit);
            for (int i = 0; i < realLimit; i++) {
                values = new ArrayList<>();
                for (int j = 0; j < channels.length; j++) {
                    if (temp[j].size() > i) {
                        values.add(temp[j].get(i));
                    }
                }
                if (values.size() > 0) {
                    result.add(values);
                }
            }
        }

        if (!singleChannel) {
            t2 = System.currentTimeMillis();
            LOG.debug("Query processing time [ms]: " + (t2 - t1));
            return result;
        }

        ChannelData data = new ChannelData(dq.getChannelName(), 0.0, System.currentTimeMillis());
        data.setNullValue();
        List<ChannelData> subResult = new ArrayList<>();
        Double actualValue = null;
        Double tmpValue;
        int size = 0;
        LOG.debug("DQ: " + dq.average + " " + dq.maximum + " " + dq.minimum + " " + dq.summary);
        if (dq.average > 0) {
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    if (i == 0) {
                        actualValue = ((ChannelData) result.get(0).get(i)).getValue();
                    } else {
                        actualValue = actualValue + ((ChannelData) result.get(0).get(i)).getValue();
                    }
                }
            }
            if (dq.getNewValue() != null) {
                if (null != actualValue) {
                    actualValue = actualValue + dq.getNewValue();
                } else {
                    actualValue = dq.getNewValue();
                }
                data.setValue(actualValue / (size + 1));
            } else {
                if (size > 0) {
                    data.setValue(actualValue / size);
                }
            }
            subResult.add(data);
            result.clear();
            result.add(subResult);
        } else if (dq.maximum > 0) {
            actualValue = Double.MIN_VALUE;
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    tmpValue = ((ChannelData) result.get(0).get(i)).getValue();
                    if (tmpValue.compareTo(actualValue) > 0) {
                        actualValue = tmpValue;
                    }
                }
            }
            if (dq.getNewValue() != null && dq.getNewValue() > actualValue) {
                actualValue = dq.getNewValue();
            }
            if (actualValue.compareTo(Double.MIN_VALUE) > 0) {
                data.setValue(actualValue);
            }
            subResult.add(data);
            result.clear();
            result.add(subResult);
        } else if (dq.minimum > 0) {
            actualValue = Double.MAX_VALUE;
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    tmpValue = ((ChannelData) result.get(0).get(i)).getValue();
                    if (tmpValue.compareTo(actualValue) < 0) {
                        actualValue = tmpValue;
                    }
                }
            }
            if (dq.getNewValue() != null && dq.getNewValue() < actualValue) {
                actualValue = dq.getNewValue();
            }
            if (actualValue.compareTo(Double.MAX_VALUE) < 0) {
                data.setValue(actualValue);
            }
            subResult.add(data);
            result.clear();
            result.add(subResult);
        } else if (dq.summary > 0) {
            actualValue = null;
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    if (i == 0) {
                        actualValue = ((ChannelData) result.get(0).get(i)).getValue();
                    } else {
                        actualValue = actualValue + ((ChannelData) result.get(0).get(i)).getValue();
                    }
                }
            }
            if (dq.getNewValue() != null) {
                if (null == actualValue) {
                    actualValue = actualValue + dq.getNewValue();
                } else {
                    actualValue = dq.getNewValue();
                }
            }
            if (null != actualValue) {
                data.setValue(actualValue);
            }
            subResult.add(data);
            result.clear();
            result.add(subResult);
        }
        t2 = System.currentTimeMillis();
        LOG.debug("Query processing time [ms]: " + (t2 - t1));
        return result;
    }

    private List<List> getValues(String userID, String deviceEUI, int limit, DataQuery dataQuery)
            throws IotDatabaseException {
        String query = SqlQueryBuilder.buildDeviceDataQuery(-1, dataQuery);
        List<String> channels = getDeviceChannels(deviceEUI);
        List<List> result = new ArrayList<>();
        ArrayList<ChannelData> row;
        ArrayList row2;
        // System.out.println("SQL QUERY: " + query);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            int paramIdx = 2;
            if (null != dataQuery.getProject()) {
                pst.setString(paramIdx, dataQuery.getProject());
                paramIdx++;
                if (null != dataQuery.getState()) {
                    pst.setDouble(paramIdx, dataQuery.getState());
                    paramIdx++;
                }
            } else {
                if (null != dataQuery.getState()) {
                    pst.setDouble(paramIdx, dataQuery.getState());
                    paramIdx++;
                }
            }
            if (null != dataQuery.getFromTs() && null != dataQuery.getToTs()) {
                LOG.debug("fromTS: " + dataQuery.getFromTs().getTime());
                pst.setTimestamp(paramIdx, dataQuery.getFromTs());
                paramIdx++;
                LOG.debug("toTS: " + dataQuery.getToTs().getTime());
                pst.setTimestamp(paramIdx, dataQuery.getToTsExclusive());
                paramIdx++;
            } else {
                LOG.debug("fromTS: " + dataQuery.getFromTs());
            }
            pst.setInt(paramIdx, dataQuery.getLimit() == 0 ? limit : dataQuery.getLimit());

            ResultSet rs = pst.executeQuery();
            if (dataQuery.isTimeseries()) {
                row2 = new ArrayList();
                row2.add("timestamp");
                for (int i = 0; i < channels.size(); i++) {
                    row2.add(channels.get(i));
                }
                result.add(row2);
            }
            double d;
            while (rs.next()) {
                if (dataQuery.isTimeseries()) {
                    row2 = new ArrayList();
                    row2.add(rs.getTimestamp(5).getTime());
                    for (int i = 0; i < channels.size(); i++) {
                        d = rs.getDouble(6 + i);
                        if (!rs.wasNull()) {
                            row2.add(d);
                        } else {
                            row2.add(null);
                        }
                    }
                    result.add(row2);
                } else {
                    row = new ArrayList<>();
                    for (int i = 0; i < channels.size(); i++) {
                        d = rs.getDouble(6 + i);
                        if (!rs.wasNull()) {
                            row.add(new ChannelData(deviceEUI, channels.get(i), d,
                                    rs.getTimestamp(5).getTime()));
                        }
                    }
                    result.add(row);
                }
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
    }

    private List<ChannelData> getChannelValues(String userID, String deviceEUI, String channel, int resultsLimit,
            DataQuery dataQuery) throws IotDatabaseException {
        ArrayList<ChannelData> result = new ArrayList<>();
        int channelIndex = getChannelIndex(deviceEUI, channel);
        if (channelIndex < 1) {
            return result;
        }
        String query = SqlQueryBuilder.buildDeviceDataQuery(channelIndex, dataQuery);
        int limit = resultsLimit;
        if (requestLimit > 0 && requestLimit < limit) {
            limit = (int) requestLimit;
        }
        LOG.debug("SQL QUERY: " + query);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);

            int paramIdx = 2;
            if (null != dataQuery.getProject()) {
                pst.setString(paramIdx, dataQuery.getProject());
                paramIdx++;
                if (null != dataQuery.getState()) {
                    pst.setDouble(paramIdx, dataQuery.getState());
                    paramIdx++;
                }
            } else {
                if (null != dataQuery.getState()) {
                    pst.setDouble(paramIdx, dataQuery.getState());
                    paramIdx++;
                }
            }
            if (null != dataQuery.getFromTs() && null != dataQuery.getToTs()) {
                pst.setTimestamp(paramIdx, dataQuery.getFromTs());
                paramIdx++;
                pst.setTimestamp(paramIdx, dataQuery.getToTsExclusive());
                paramIdx++;
            }
            pst.setInt(paramIdx, limit);

            ResultSet rs = pst.executeQuery();
            Double d;
            while (rs.next()) {
                d = rs.getDouble(6);
                if (!rs.wasNull()) {
                    result.add(0, new ChannelData(deviceEUI, channel, d, rs.getTimestamp(5).getTime()));
                }
            }
            return result;
        } catch (SQLException e) {
            LOG.error("problematic query = " + query);
            e.printStackTrace();
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
    }

    private List<List> getVirtualDeviceMeasures(String userID, String deviceEUI, DataQuery dataQuery)
            throws IotDatabaseException {
        List<List> result = new ArrayList<>();
        String query = SqlQueryBuilder.buildDeviceDataQuery(-1, dataQuery);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            String eui;
            Timestamp ts;
            String serializedData;
            ChannelData cData;
            ArrayList<ChannelData> channels = new ArrayList<>();
            String channelName;
            while (rs.next()) {
                eui = rs.getString(1);
                ts = rs.getTimestamp(2);
                serializedData = rs.getString(3);
                JsonObject jo = (JsonObject) JsonReader.jsonToJava(serializedData);
                VirtualData vd = new VirtualData(eui);
                vd.timestamp = ts.getTime();
                JsonObject fields = (JsonObject) jo.get("payload_fields");
                Iterator<String> it = fields.keySet().iterator();
                while (it.hasNext()) {
                    channelName = it.next();
                    cData = new ChannelData();
                    cData.setDeviceEUI(eui);
                    cData.setTimestamp(vd.timestamp);
                    cData.setName(channelName);
                    cData.setValue((Double) fields.get(channelName));
                    channels.add(cData);
                }
            }
            result.add(channels);
        } catch (SQLException e) {
            LOG.error("problematic query = " + query);
            e.printStackTrace();
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return result;
    }

    @Override
    public ChannelData getMinimalValue(String userID, String deviceID, String channel, int scope, Double newValue)
            throws IotDatabaseException {
        ArrayList<Double> list = getLastValues(deviceID, channel, scope);
        if (null != newValue) {
            list.add(newValue);
        }
        if (list.size() == 0) {
            ChannelData cd = new ChannelData();
            cd.setNullValue();
            cd.setName(channel);
            cd.setTimestamp(System.currentTimeMillis());
            return cd;
        }
        double result = list.get(0).doubleValue();
        double nextValue;
        for (int i = 1; i < list.size(); i++) {
            nextValue = list.get(i).doubleValue();
            if (result > nextValue) {
                result = nextValue;
            }
        }
        return new ChannelData(channel, result, System.currentTimeMillis());
    }

    @Override
    public ChannelData getMaximalValue(String userID, String deviceID, String channel, int scope, Double newValue)
            throws IotDatabaseException {
        ArrayList<Double> list = getLastValues(deviceID, channel, scope);
        if (null != newValue) {
            list.add(newValue);
        }
        if (list.size() == 0) {
            ChannelData cd = new ChannelData();
            cd.setNullValue();
            cd.setName(channel);
            cd.setTimestamp(System.currentTimeMillis());
            return cd;
        }
        double result = list.get(0).doubleValue();
        double nextValue;
        for (int i = 1; i < list.size(); i++) {
            nextValue = list.get(i).doubleValue();
            if (result < nextValue) {
                result = nextValue;
            }
        }
        return new ChannelData(channel, result, System.currentTimeMillis());
    }

    @Override
    public ChannelData getSummaryValue(String userID, String deviceID, String channel, int scope, Double newValue)
            throws IotDatabaseException {
        ArrayList<Double> list = getLastValues(deviceID, channel, scope);
        if (null != newValue) {
            list.add(newValue);
        }
        if (list.size() == 0) {
            ChannelData cd = new ChannelData();
            cd.setNullValue();
            cd.setName(channel);
            cd.setTimestamp(System.currentTimeMillis());
            return cd;
        }
        Double result = 0.0;
        for (int i = 0; i < list.size(); i++) {
            result = Double.sum(result, list.get(i));
        }
        return new ChannelData(channel, result, System.currentTimeMillis());
    }

    @Override
    public ChannelData getAverageValue(String userID, String deviceID, String channel, int scope, Double newValue)
            throws IotDatabaseException {
        ArrayList<Double> list = getLastValues(deviceID, channel, scope);
        if (null != newValue) {
            list.add(newValue);
        }
        if (list.size() == 0) {
            ChannelData cd = new ChannelData();
            cd.setNullValue();
            cd.setName(channel);
            cd.setTimestamp(System.currentTimeMillis());
            return cd;
        }
        Double result = 0.0;
        for (int i = 0; i < list.size(); i++) {
            result = Double.sum(result, list.get(i));
        }
        result = result / list.size();
        return new ChannelData(channel, result, System.currentTimeMillis());
    }

    private ArrayList<Double> getLastValues(String deviceEUI, String channel, int scope) throws IotDatabaseException {
        ArrayList<Double> result = new ArrayList<>();
        int channelIndex = getChannelIndex(deviceEUI, channel);
        if (channelIndex <= 0) {
            return result;
        }
        String columnName = "d" + channelIndex;
        String query = "select " + columnName + " from devicedata where eui=? order by tstamp desc limit ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            pst.setInt(2, scope);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                result.add(rs.getDouble(1));
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public List<List> getLastValues(String userID, String deviceEUI) throws IotDatabaseException {
        String query = "select eui,userid,day,dtime,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24 from devicedata where eui=? order by tstamp desc limit 1";
        List<String> channels = getDeviceChannels(deviceEUI);
        ArrayList<ChannelData> row = new ArrayList<>();
        ArrayList<List> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            double d;
            if (rs.next()) {
                for (int i = 0; i < channels.size(); i++) {
                    d = rs.getDouble(6 + i);
                    if (!rs.wasNull()) {
                        row.add(new ChannelData(deviceEUI, channels.get(i), d,
                                rs.getTimestamp(5).getTime()));
                    }
                }
                result.add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        LOG.info("createStructure()");
        String query;
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SEQUENCE IF NOT EXISTS id_seq;");
        // applications
        sb.append("CREATE TABLE IF NOT EXISTS applications (")
                .append("id bigint default id_seq.nextval primary key,")
                // .append("organization bigint default 0 references organizations,")
                .append("organization bigint default 0,")
                .append("version bigint default 0,")
                .append("name varchar UNIQUE, configuration varchar);");
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(sb.toString());) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn
                        .prepareStatement("INSERT INTO applications values (0,0,0,'system','{}');");) {
            pst.executeUpdate();
        } catch (SQLException e) {

        }
        sb = new StringBuilder();
        // devicetemplates
        sb.append("CREATE TABLE IF NOT EXISTS devicetemplates (").append("eui varchar primary key,")
                .append("appid varchar,")
                .append("appeui varchar,").append("type varchar,").append("channels varchar,")
                .append("code varchar,").append("decoder varchar,").append("description varchar,")
                .append("tinterval bigint,").append("pattern varchar,").append("commandscript varchar,")
                .append("producer varchar,").append("configuration varchar);");
        // dashboardtemplates
        sb.append("CREATE TABLE IF NOT EXISTS dashboardtemplates (").append("id varchar primary key,")
                .append("title varchar,")
                .append("widgets varchar);");
        // devices
        sb.append("CREATE TABLE IF NOT EXISTS devices (")
                .append("eui varchar primary key,")
                .append("name varchar,")
                .append("userid varchar,")
                .append("type varchar,")
                .append("team varchar,")
                .append("channels varchar,")
                .append("code varchar,")
                .append("decoder varchar,")
                .append("devicekey varchar,")
                .append("description varchar,")
                .append("lastseen bigint,")
                .append("tinterval bigint,")
                .append("lastframe bigint,")
                .append("template varchar,")
                .append("pattern varchar,")
                .append("downlink varchar,")
                .append("commandscript varchar,")
                .append("appid varchar,")
                .append("groups varchar,")
                .append("alert number,")
                .append("appeui varchar,")
                .append("devid varchar,")
                .append("active boolean,")
                .append("project varchar,")
                .append("latitude double,")
                .append("longitude double,")
                .append("altitude double,")
                .append("state double,")
                .append("retention bigint,")
                .append("administrators varchar,")
                .append("framecheck boolean,")
                .append("configuration varchar,")
                .append("organization bigint default 0,")
                .append("organizationapp bigint default 0 references applications);");
        // dashboards
        sb.append("CREATE TABLE IF NOT EXISTS dashboards (").append("id varchar primary key,")
                .append("name varchar,")
                .append("userid varchar,").append("title varchar,").append("team varchar,")
                .append("widgets varchar,").append("token varchar,").append("shared boolean,")
                .append("administrators varchar);");
        // alerts
        sb.append("CREATE TABLE IF NOT EXISTS alerts (").append("id bigint default id_seq.nextval primary key ,")
                .append("name varchar,")
                .append("category varchar,").append("type varchar,").append("deviceeui varchar,")
                .append("userid varchar,").append("payload varchar,").append("timepoint varchar,")
                .append("serviceid varchar,").append("uuid varchar,").append("calculatedtimepoint bigint,")
                .append("createdat bigint,").append("rooteventid bigint,").append("cyclic boolean);");
        // devicechannels
        sb.append("CREATE TABLE IF NOT EXISTS devicechannels (")
                .append("eui varchar primary key,")
                .append("channels varchar);");
        // devicedata
        sb.append("CREATE TABLE IF NOT EXISTS devicedata (").append("eui varchar not null,").append("userid varchar,")
                .append("day date,").append("dtime time,").append("tstamp timestamp not null,").append("d1 double,")
                .append("d2 double,").append("d3 double,").append("d4 double,").append("d5 double,")
                .append("d6 double,").append("d7 double,").append("d8 double,").append("d9 double,")
                .append("d10 double,").append("d11 double,").append("d12 double,").append("d13 double,")
                .append("d14 double,").append("d15 double,").append("d16 double,").append("d17 double,")
                .append("d18 double,").append("d19 double,").append("d20 double,").append("d21 double,")
                .append("d22 double,").append("d23 double,").append("d24 double,").append("project varchar,")
                .append("state double,")
                .append("PRIMARY KEY (eui,tstamp) );");
        // virtualdevicedata
        sb.append("CREATE TABLE IF NOT EXISTS virtualdevicedata (")
                .append("eui VARCHAR PRIMARY KEY,tstamp TIMESTAMP NOT NULL, data VARCHAR);");
        // groups
        sb.append("CREATE TABLE IF NOT EXISTS groups (").append("eui varchar primary key,").append("name varchar,")
                .append("userid varchar,").append("team varchar,").append("channels varchar,")
                .append("description varchar,")
                .append("administrators varchar,")
                .append("organization bigint default 0);");
        // commands
        sb.append("CREATE TABLE IF NOT EXISTS commands (")
                .append("id bigint,")
                .append("category varchar,")
                .append("type varchar,")
                .append("origin varchar,")
                .append("payload varchar,")
                .append("createdat bigint);");
        sb.append("CREATE INDEX IF NOT EXISTS idxcommands on commands(origin);");
        // commandslog
        sb.append("CREATE TABLE IF NOT EXISTS commandslog (")
                .append("id bigint,")
                .append("category varchar,")
                .append("type varchar,")
                .append("origin varchar,")
                .append("payload varchar,")
                .append("createdat bigint);");
        sb.append("CREATE INDEX IF NOT EXISTS idxcommandslog on commandslog(origin);");
        query = sb.toString();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        query = "CREATE INDEX IF NOT EXISTS idx_devicedata_eui_tstamp on devicedata(eui,tstamp);"
                + "CREATE INDEX IF NOT EXISTS idx_devicedata_tstamp on devicedata(tstamp)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        // TODO: devicestatus
        query = "CREATE TABLE IF NOT EXISTS devicestatus ( "
                + "eui VARCHAR NOT NULL,"
                + "tinterval BIGINT,"
                + "ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," // lastseen from ts
                + "status DOUBLE,"
                + "alert NUMBER"
                + ");"
                + "CREATE INDEX IF NOT EXISTS idx_devicestatus_eui_ts on devicestatus(eui,ts);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        query = "CREATE TABLE IF NOT EXISTS account_params "
                + "(param VARCHAR, accounttype INTEGER, text VARCHAR, value BIGINT); "
                + "CREATE TABLE IF NOT EXISTS account_features "
                + "(feature VARCHAR, accounttype INTEGER, enabled BOOLEAN);"
                + "CREATE INDEX IF NOT EXISTS idx_account_params on account_params(param,accounttype);"
                + "CREATE INDEX IF NOT EXISTS idx_account_features on account_features(feature,accounttype);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }

        query = "CREATE TABLE IF NOT EXISTS favourites ("
                + "userid VARCHAR,"
                + "id VARCHAR,"
                + "is_device BOOLEAN," // true - device, false - dashboard
                + "PRIMARY KEY (userid,id,is_device));";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    /**
     * Get list of devices accessible for user.
     * This method should not be used for users with organization different than
     * default.
     * 
     * @param userID    - user ID
     * @param deviceEUI - device EUI
     * @param channel   - channel name
     * @param scope     - scope
     * @param newValue  - new value
     * @return list of device data
     */
    @Override
    public List<Device> getUserDevices(User user, boolean withStatus, Integer limit, Integer offset)
            throws IotDatabaseException {
        ArrayList<Device> devices = new ArrayList<>();

        if (user.organization != defaultOrganizationId) {
            return devices;
        }
        // TODO: withShared, withStatus
        DeviceSelector selector = new DeviceSelector(user, false, withStatus, false, limit, offset);
        String query = selector.query;
        Device device;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            if (selector.numberOfWritableParams > 0) {
                pst.setString(1, user.uid);
                pst.setString(2, user.uid);
            }
            if (selector.numberOfUserParams > 0) {
                pst.setString(selector.numberOfWritableParams + 1, user.uid);
                pst.setString(selector.numberOfWritableParams + 2, "%," + user.uid + ",%");
                pst.setString(selector.numberOfWritableParams + 3, "%," + user.uid + ",%");
            }
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                device = buildDevice(rs);
                if (withStatus) {
                    device = getDeviceStatusData(device);
                }
                devices.add(device);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return devices;
    }

    @Override
    public List<Device> getOrganizationDevices(long organizationId, boolean withStatus, Integer limit, Integer offset)
            throws IotDatabaseException {
        ArrayList<Device> devices = new ArrayList<>();
        String query = "SELECT * FROM devices WHERE organization=? LIMIT ? OFFSET ?";
        Device device;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setInt(2, limit);
            pst.setInt(3, offset);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                device = buildDevice(rs);
                if (withStatus) {
                    device = getDeviceStatusData(device);
                }
                devices.add(device);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return devices;
    }

    @Override
    public Device getDevice(User user, String deviceEUI, boolean withShared, boolean withStatus)
            throws IotDatabaseException {
        // TODO: withShared, withStatus
        DeviceSelector selector = new DeviceSelector(user, withShared, withStatus, true, null, null);
        String query = selector.query;
        LOG.info(query);
        Device device = null;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            if (selector.numberOfWritableParams > 0) {
                pst.setString(1, user.uid);
                pst.setString(2, user.uid);
            }
            pst.setString(selector.numberOfWritableParams + 1, deviceEUI);
            if (selector.numberOfUserParams > 0) {
                pst.setString(selector.numberOfWritableParams + 2, user.uid);
                pst.setString(selector.numberOfWritableParams + 3, "%," + user.uid + ",%");
                pst.setString(selector.numberOfWritableParams + 4, "%," + user.uid + ",%");
            }
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                device = buildDevice(rs);
                if (withStatus) {
                    device = getDeviceStatusData(device);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return device;
    }

    private Device buildDevice(ResultSet rs) throws SQLException {
        Device device = new Device();
        device.setEUI(rs.getString("eui"));
        device.setName(rs.getString("name"));
        device.setUserID(rs.getString("userid"));
        device.setType(rs.getString("type"));
        device.setTeam(rs.getString("team"));
        device.setChannels(rs.getString("channels"));
        device.setCode(rs.getString("code"));
        device.setEncoder(rs.getString("decoder"));
        device.setKey(rs.getString("devicekey"));
        device.setDescription(rs.getString("description"));
        device.setTransmissionInterval(rs.getLong("tinterval"));
        device.setTemplate(rs.getString("template"));
        device.setPattern(rs.getString("pattern"));
        device.setCommandScript(rs.getString("commandscript"));
        device.setGroups(rs.getString("groups"));
        device.setApplicationID(rs.getString("appid"));
        device.setApplicationEUI(rs.getString("appeui"));
        device.setDeviceID(rs.getString("devid"));
        device.setActive(rs.getBoolean("active"));
        device.setProject(rs.getString("project"));
        device.setLatitude(rs.getDouble("latitude"));
        device.setLongitude(rs.getDouble("longitude"));
        device.setAltitude(rs.getDouble("altitude"));
        device.setRetentionTime(rs.getLong("retention"));
        device.setAdministrators(rs.getString("administrators"));
        device.setCheckFrames(rs.getBoolean("framecheck"));
        device.setConfiguration(rs.getString("configuration"));
        device.setOrganizationId(rs.getLong("organization"));
        device.setOrgApplicationId(rs.getLong("organizationapp"));
        try {
            device.setWritable(rs.getBoolean("writable"));
        } catch (Exception e) {
            device.setWritable(true); // writable won't be used in new access logic
        }
        return device;
    }

    @Override
    public void deleteDevice(User user, String deviceEUI) throws IotDatabaseException {
        LOG.info("deleteDevice: " + deviceEUI + " for user: " + user.uid);
        Device device = getDevice(user, deviceEUI, false, false);
        if (!device.isWritable()) {
            throw new IotDatabaseException(IotDatabaseException.CONFLICT, "User is not allowed to update device");
        }
        String query = "DELETE FROM devices WHERE eui=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void updateDevice(User user, Device updatedDevice) throws IotDatabaseException {
        Device device = getDevice(user, updatedDevice.getEUI(), true, false);
        if (!device.isWritable()) {
            throw new IotDatabaseException(IotDatabaseException.CONFLICT, "User is not allowed to update device");
        }
        String query = "UPDATE devices SET name=?, userid=?, type=?, team=?, channels=?, code=?, "
                + "decoder=?, devicekey=?, description=?, tinterval=?, template=?, pattern=?, "
                + "commandscript=?, appid=?, groups=?, appeui=?, devid=?, active=?, project=?, "
                + "latitude=?, longitude=?, altitude=?, retention=?, administrators=?, "
                + "framecheck=?, configuration=?, organization=?, organizationapp=? "
                + "WHERE eui=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, updatedDevice.getName());
            pst.setString(2, updatedDevice.getUserID());
            pst.setString(3, updatedDevice.getType());
            pst.setString(4, updatedDevice.getTeam());
            pst.setString(5, updatedDevice.getChannelsAsString());
            pst.setString(6, updatedDevice.getCode());
            pst.setString(7, updatedDevice.getEncoder());
            pst.setString(8, updatedDevice.getKey());
            pst.setString(9, updatedDevice.getDescription());
            pst.setLong(10, updatedDevice.getTransmissionInterval());
            pst.setString(11, updatedDevice.getTemplate());
            pst.setString(12, updatedDevice.getPattern());
            pst.setString(13, updatedDevice.getCommandScript());
            pst.setString(14, updatedDevice.getApplicationID());
            pst.setString(15, updatedDevice.getGroups());
            pst.setString(16, updatedDevice.getApplicationEUI());
            pst.setString(17, updatedDevice.getDeviceID());
            pst.setBoolean(18, updatedDevice.isActive());
            pst.setString(19, updatedDevice.getProject());
            if (null != updatedDevice.getLatitude()) {
                pst.setDouble(20, updatedDevice.getLatitude());
            } else {
                pst.setNull(20, java.sql.Types.DOUBLE);
            }
            if (null != updatedDevice.getLongitude()) {
                pst.setDouble(21, updatedDevice.getLongitude());
            } else {
                pst.setNull(21, java.sql.Types.DOUBLE);
            }
            if (null != updatedDevice.getAltitude()) {
                pst.setDouble(22, updatedDevice.getAltitude());
            } else {
                pst.setNull(22, java.sql.Types.DOUBLE);
            }
            pst.setLong(23, updatedDevice.getRetentionTime());
            pst.setString(24, updatedDevice.getAdministrators());
            pst.setBoolean(25, updatedDevice.isCheckFrames());
            pst.setString(26, updatedDevice.getConfiguration());
            if (null != updatedDevice.getOrganizationId()) {
                pst.setLong(27, updatedDevice.getOrganizationId());
            } else {
                pst.setLong(27, defaultOrganizationId);
            }
            if (null != updatedDevice.getOrgApplicationId()) {
                pst.setLong(28, updatedDevice.getOrgApplicationId());
            } else {
                pst.setLong(28, defaultApplicationId);
            }
            pst.setString(29, updatedDevice.getEUI());
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void createDevice(User user, Device device) throws IotDatabaseException {
        LOG.info("createDevice: " + device.getEUI() + " for user: " + user.uid);
        String query = "INSERT INTO devices (eui, name, userid, type, team, channels, code, "
                + "decoder, devicekey, description, tinterval, template, pattern, "
                + "commandscript, appid, groups, appeui, devid, active, project, "
                + "latitude, longitude, altitude, retention, administrators, "
                + "framecheck, configuration, organization, organizationapp) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, device.getEUI());
            pst.setString(2, device.getName());
            pst.setString(3, device.getUserID());
            pst.setString(4, device.getType());
            pst.setString(5, device.getTeam());
            pst.setString(6, device.getChannelsAsString());
            pst.setString(7, device.getCode());
            pst.setString(8, device.getEncoder());
            pst.setString(9, device.getKey());
            pst.setString(10, device.getDescription());
            pst.setLong(11, device.getTransmissionInterval());
            pst.setString(12, device.getTemplate());
            pst.setString(13, device.getPattern());
            pst.setString(14, device.getCommandScript());
            pst.setString(15, device.getApplicationID());
            pst.setString(16, device.getGroups());
            pst.setString(17, device.getApplicationEUI());
            pst.setString(18, device.getDeviceID());
            pst.setBoolean(19, device.isActive());
            pst.setString(20, device.getProject());
            if (null != device.getLatitude()) {
                pst.setDouble(21, device.getLatitude());
            } else {
                pst.setNull(21, java.sql.Types.DOUBLE);
            }
            if (null != device.getLongitude()) {
                pst.setDouble(22, device.getLongitude());
            } else {
                pst.setNull(22, java.sql.Types.DOUBLE);
            }
            if (null != device.getAltitude()) {
                pst.setDouble(23, device.getAltitude());
            } else {
                pst.setNull(23, java.sql.Types.DOUBLE);
            }
            pst.setLong(24, device.getRetentionTime());
            pst.setString(25, device.getAdministrators());
            pst.setBoolean(26, device.isCheckFrames());
            pst.setString(27, device.getConfiguration());
            if (null != device.getOrganizationId()) {
                pst.setLong(28, device.getOrganizationId());
            } else {
                pst.setLong(28, defaultOrganizationId);
            }
            if (null != device.getOrgApplicationId()) {
                pst.setLong(29, device.getOrgApplicationId());
            } else {
                pst.setLong(29, defaultApplicationId);
            }
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    // @Override
    public List<Device> getInactiveDevices_actual() throws IotDatabaseException {
        ArrayList<DevStamp> stamps = new ArrayList<DevStamp>();
        ArrayList<Device> result = new ArrayList<Device>();
        ArrayList<Device> devices = new ArrayList<Device>();
        // find active devices with tinterval > 0
        DeviceSelector selector = new DeviceSelector();
        String query = selector.query;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query)) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                Device device = new Device();
                device.setEUI(rs.getString("eui"));
                device.setName(rs.getString("name"));
                device.setUserID(rs.getString("userid"));
                device.setType(rs.getString("type"));
                device.setTeam(rs.getString("team"));
                device.setChannels(rs.getString("channels"));
                device.setCode(rs.getString("code"));
                device.setEncoder(rs.getString("decoder"));
                device.setKey(rs.getString("devicekey"));
                device.setDescription(rs.getString("description"));
                device.setTransmissionInterval(rs.getLong("tinterval"));
                device.setTemplate(rs.getString("template"));
                device.setPattern(rs.getString("pattern"));
                device.setCommandScript(rs.getString("commandscript"));
                device.setApplicationID(rs.getString("appid"));
                device.setGroups(rs.getString("groups"));
                device.setApplicationEUI(rs.getString("appeui"));
                device.setDeviceID(rs.getString("devid"));
                device.setActive(rs.getBoolean("active"));
                device.setProject(rs.getString("project"));
                device.setLatitude(rs.getDouble("latitude"));
                device.setLongitude(rs.getDouble("longitude"));
                device.setAltitude(rs.getDouble("altitude"));
                device.setRetentionTime(rs.getLong("retention"));
                device.setAdministrators(rs.getString("administrators"));
                device.setCheckFrames(rs.getBoolean("framecheck"));
                device.setConfiguration(rs.getString("configuration"));
                device.setOrganizationId(rs.getLong("organization"));
                device.setOrgApplicationId(rs.getLong("organizationapp"));
                devices.add(device);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }

        String query2 = "SELECT eui,ts,alert FROM device_status WHERE eui = ? ORDER BY ts DESC LIMIT 1";
        for (int i = 0; i < devices.size(); i++) {
            try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query2);) {
                Device device = devices.get(i);
                pst.setString(1, device.getEUI());
                ResultSet rs = pst.executeQuery();
                if (rs.next()) {
                    DevStamp stamp = new DevStamp();
                    stamp.setEui(rs.getString("eui"));
                    stamp.setTs(rs.getTimestamp("ts"));
                    stamp.setAlert(rs.getInt("alert"));
                    stamps.add(stamp);
                    if (stamp.alert < 2 && stamp.ts.before(
                            new Timestamp(System.currentTimeMillis() - device.getTransmissionInterval() * 1000))) {
                        result.add(device);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
                throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
            }
        }
        return result;
    }

    @Override
    public List<Device> getInactiveDevices() throws IotDatabaseException {
        DeviceSelector selector = new DeviceSelector(true);
        String query = selector.query;
        Device device;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            ResultSet rs = pstmt.executeQuery();
            ArrayList<Device> list = new ArrayList<>();
            while (rs.next()) {
                device = buildDevice(rs);
                device = getDeviceStatusData(device);
                list.add(device);
            }
            return list;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    class DevStamp {
        String eui;
        Timestamp ts;
        int alert;

        void setEui(String eui) {
            this.eui = eui;
        }

        void setTs(Timestamp ts) {
            this.ts = ts;
        }

        void setAlert(int alert) {
            this.alert = alert;
        }
    }

    @Override
    public long getParameterValue(String name, long accountType) throws IotDatabaseException {
        String query = "SELECT value FROM account_params WHERE param=? AND accounttype=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getLong("value");
            }
            return -1;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public String getParameterTextValue(String name, long accountType) throws IotDatabaseException {
        String query = "SELECT text FROM account_params WHERE param=? AND accounttype=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString("text");
            }
            return null;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void setParameter(String name, long accountType, long value, String text) throws IotDatabaseException {
        String query = "MERGE INTO account_params (param, accounttype, value, text) KEY(param,accounttype) VALUES (?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            pstmt.setLong(3, value);
            pstmt.setString(4, text);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public boolean isFeatureEnabled(String name, long accountType) throws IotDatabaseException {
        String query = "SELECT enabled FROM account_features WHERE feature=? AND accounttype=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getBoolean("enabled");
            }
            return false;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void setFeature(String name, long accountType, boolean enabled) throws IotDatabaseException {
        String query = "MERGE INTO account_features (feature, accounttype, enabled) KEY(feature,accounttype) VALUES (?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            pstmt.setBoolean(3, enabled);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void clearDeviceData(String deviceEUI) throws IotDatabaseException {
        String query = "DELETE FROM devicedata WHERE eui=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void updateDeviceChannels(String deviceEUI, String channels) throws IotDatabaseException {
        String query = "MERGE INTO devicechannels (eui, channels) KEY(eui) VALUES (?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            pstmt.setString(2, channels);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void addFavouriteDevice(String userID, String eui) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addFavouriteDevice'");
    }

    @Override
    public void removeFavouriteDevices(String userID, String eui) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'removeFavouriteDevices'");
    }

    @Override
    public List<Device> getFavouriteDevices(String userID) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFavouriteDevices'");
    }
}
