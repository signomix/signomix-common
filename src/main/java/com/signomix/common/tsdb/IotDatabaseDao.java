package com.signomix.common.tsdb;

import com.cedarsoftware.util.io.JsonObject;
import com.cedarsoftware.util.io.JsonReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.signomix.common.Tag;
import com.signomix.common.User;
import com.signomix.common.db.DataQuery;
import com.signomix.common.db.DataQueryException;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.Alert;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.CommandDto;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.DeviceGroup;
import com.signomix.common.iot.DeviceStatusDto;
import com.signomix.common.iot.DeviceTemplate;
import com.signomix.common.iot.virtual.VirtualData;
import io.agroal.api.AgroalDataSource;
import io.quarkus.cache.CacheResult;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import org.jboss.logging.Logger;

@Singleton
public class IotDatabaseDao implements IotDatabaseIface {

    private static final Logger logger = Logger.getLogger(IotDatabaseDao.class);

    Long defaultOrganizationId = 1L;
    Long defaultApplicationId = 1L;

    private AgroalDataSource dataSource;
    private AgroalDataSource analyticDataSource;

    // TODO: get requestLimit from config
    private long requestLimit = 500;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void setAnalyticDatasource(AgroalDataSource dataSource) {
        this.analyticDataSource = dataSource;
    }

    @Override
    public void setQueryResultsLimit(int limit) {
        requestLimit = limit;
        if (logger.isDebugEnabled()) {
            logger.debug("requestLimit:" + requestLimit);
        }
    }

    @Override
    public long getNextId(String tableName, String columnName)
            throws IotDatabaseException {
        String query = "SELECT nextval(pg_get_serial_sequence('"
                + tableName
                + "', '"
                + columnName
                + "'))";
        long result = 0;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    result = rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return result;
    }

    @Override
    public ChannelData getLastValue(
            String userID,
            String deviceEUI,
            String channel,
            boolean skipNull) throws IotDatabaseException {
        int channelIndex = getChannelIndex(deviceEUI, channel);
        if (channelIndex < 0) {
            return null;
        }
        String columnName = "d" + (channelIndex);
        String query = "select eui,userid,tstamp,"
                + columnName
                + " from analyticdata where eui=? order by tstamp desc limit 1";
        if (skipNull) {
            query = "select eui,userid,tstamp,"
                    + columnName
                    + " from analyticdata where eui=? and "
                    + columnName
                    + " is not null order by tstamp desc limit 1";
        }
        if (deviceEUI.equalsIgnoreCase("DKHSROOM311")) {
            logger.info("DKHSROOM311 last value query: " + query);
        }
        ChannelData result = null;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            try (ResultSet rs = pst.executeQuery();) {
                Double d;
                if (rs.next()) {
                    d = rs.getDouble(4);
                    if (!rs.wasNull()) {
                        result = new ChannelData(
                                deviceEUI,
                                channel,
                                d,
                                rs.getTimestamp(3).getTime());
                    }
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        } catch (Exception e1) {
            throw new IotDatabaseException(
                    IotDatabaseException.UNKNOWN,
                    e1.getMessage());
        }
        return result;
    }

    @Deprecated
    public List<List<List>> getValuesOfGroup(
            String userID,
            long organizationId,
            String groupEUI,
            String channelNames,
            long secondsBack) throws IotDatabaseException {
        String[] channels = channelNames.split(",");
        return getGroupLastValues(
                userID,
                organizationId,
                groupEUI,
                channels,
                secondsBack);
    }

    public List<String> getGroupChannels(String groupEUI)
            throws IotDatabaseException {
        List<String> channels;
        // return ((Service) Kernel.getInstance()).getDataStorageAdapter().
        String query = "select channels from groups where eui=?";
        channels = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, groupEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                String tmps = rs.getString(1).toLowerCase().trim();
                if (logger.isDebugEnabled()) {
                    logger.debug("channels: " + tmps);
                }
                String[] s = tmps.split(",");
                channels = Arrays.asList(s);
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return channels;
    }

    @Override
    public List<List<List>> getGroupLastValues(
            String userID,
            long organizationID,
            String groupEUI,
            String[] channelNames,
            long secondsBack) throws IotDatabaseException {
        if (logger.isDebugEnabled()) {
            logger.debug("getGroupLastValues");
        }
        List<List<List>> result = new ArrayList<>();
        List<String> requestChannels = Arrays.asList(channelNames);
        List<ChannelData> tmpResult = new ArrayList<>();
        List<List> measuresForEui = new ArrayList<>();
        List<ChannelData> measuresForEuiTimestamp = new ArrayList<>();
        ChannelData cd;
        try {
            String group = "%," + groupEUI + ",%";
            // long timestamp = System.currentTimeMillis() - secondsBack * 1000;
            String deviceQuery = "SELECT eui,channels FROM devices WHERE groups like ?;";
            HashMap<String, List> devices = new HashMap<>();
            String query;
            query = "SELECT "
                    + "eui,userid,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24 "
                    + "FROM devicedata "
                    + "WHERE eui IN "
                    + "(SELECT eui FROM devices WHERE groups like ?) "
                    + "AND tstamp > (CURRENT_TIMESTAMP - ? * INTERVAL '1 second') "
                    + "ORDER BY eui,tstamp DESC;";
            List<String> groupChannels = getGroupChannels(groupEUI);
            if (requestChannels.size() == 0) {
                logger.warn("empty channelNames");
                requestChannels = groupChannels;
            }

            // logger.debug("{} {} {} {} {}", groupEUI, group, groupChannels.size(),
            // requestChannels.size(), query);
            // .info("query withseconds back: " + query);
            try (
                    Connection conn = dataSource.getConnection();
                    PreparedStatement pstd = conn.prepareStatement(deviceQuery);
                    PreparedStatement pst = conn.prepareStatement(query);) {
                pstd.setString(1, group);
                try (ResultSet rs = pstd.executeQuery();) {
                    while (rs.next()) {
                        // logger.debug("device: "+ rs.getString(1));
                        devices.put(
                                rs.getString(1),
                                Arrays.asList(rs.getString(2).split(",")));
                    }
                    pst.setString(1, group);
                    pst.setLong(2, secondsBack);
                }
                try (ResultSet rs = pst.executeQuery();) {
                    int channelIndex;
                    String channelName;
                    String devEui;
                    double d;
                    while (rs.next()) {
                        for (int i = 0; i < groupChannels.size(); i++) {
                            devEui = rs.getString(1);
                            channelName = groupChannels.get(i);
                            channelIndex = devices
                                    .get(devEui)
                                    .indexOf(channelName);
                            if (channelIndex < 0) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug(
                                            "Channel not found: "
                                                    + channelName
                                                    + " for device: "
                                                    + devEui);
                                }
                                continue;
                            }
                            d = rs.getDouble(4 + channelIndex);
                            if (!rs.wasNull()) {
                                tmpResult.add(
                                        new ChannelData(
                                                devEui,
                                                channelName,
                                                d,
                                                rs.getTimestamp(3).getTime()));
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
                throw new IotDatabaseException(
                        IotDatabaseException.SQL_EXCEPTION,
                        e.getMessage(),
                        e);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
                ex.printStackTrace();
                throw new IotDatabaseException(
                        IotDatabaseException.UNKNOWN,
                        ex.getMessage());
            }
            if (tmpResult.isEmpty()) {
                return result;
            }
            String prevEUI = "";
            long prevTimestamp = 0;
            int idx;
            for (int i = 0; i < tmpResult.size(); i++) {
                cd = tmpResult.get(i);

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
            if (logger.isDebugEnabled()) {
                logger.debug("result: " + result.size());
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
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
        return result;
    }

    /**
     * Get values of a group of devices The resulting dataset can contain
     * maximum 31 days of data
     */
    @Override
    public List<List<List>> getGroupValues(
            String userID,
            long organizationId,
            String groupEUI,
            String[] channelNames,
            String dataQuery) throws IotDatabaseException {
        List<List<List>> result = new ArrayList<>();
        DataQuery dq = null;
        try {
            dq = DataQuery.parse(dataQuery);
        } catch (DataQueryException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.UNKNOWN,
                    "Invalid data query",
                    null);
        }
        Timestamp fromTs = dq.getFromTs();
        Timestamp toTs = dq.getToTs();
        if (fromTs == null || toTs == null || fromTs.after(toTs)) {
            throw new IotDatabaseException(
                    IotDatabaseException.UNKNOWN,
                    "Invalid dates in data query",
                    null);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("fromTs: " + fromTs + " toTs: " + toTs);
        }
        Instant i1 = fromTs.toInstant();
        Instant i2 = toTs.toInstant();
        Instant i3 = i2.minusSeconds(31 * 24 * 60 * 60);
        if (i1.isBefore(i3)) {
            fromTs = Timestamp.from(i3);
        }

        // if toTs is more than 1 month from fromTs, set fromTs to 1 month before toTs
        /*
         * long diff = toTs.getTime() - fromTs.getTime();
         * if (diff > 31 * 24 * 60 * 60 * 1000) {
         * fromTs = new Timestamp(toTs.getTime() - 31 * 24 * 60 * 60 * 1000);
         * }
         */
        if (logger.isDebugEnabled()) {
            logger.debug("fromTs_2: " + fromTs + " toTs: " + toTs);
        }
        List<String> requestChannels = Arrays.asList(channelNames);
        try {
            String group = "%," + groupEUI + ",%";
            String deviceQuery = "SELECT eui,channels FROM devices WHERE groups like ?;";
            HashMap<String, List> devices = new HashMap<>();
            String query;
            query = "SELECT "
                    + "eui,userid,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24 "
                    + "FROM devicedata "
                    + "WHERE eui IN "
                    + "(SELECT eui FROM devices WHERE groups like ?) "
                    + "AND tstamp >= ? AND tstamp <= ? "
                    + "ORDER BY eui,tstamp;";
            List<String> groupChannels = getGroupChannels(groupEUI);
            if (requestChannels.size() == 0 || requestChannels.indexOf("*") > -1) {
                if (logger.isDebugEnabled()) {
                    logger.debug("empty channelNames");
                }
                requestChannels = groupChannels;
            }

            List<List> measuresForEui = new ArrayList<>();
            List<ChannelData> measuresForEuiTimestamp = new ArrayList<>();
            List<ChannelData> tmpResult = new ArrayList<>();
            ChannelData cd;
            try (
                    Connection conn = dataSource.getConnection();
                    PreparedStatement pstd = conn.prepareStatement(deviceQuery);
                    PreparedStatement pst = conn.prepareStatement(query);) {
                pstd.setString(1, group);
                try (ResultSet rs = pstd.executeQuery();) {
                    while (rs.next()) {
                        devices.put(
                                rs.getString(1),
                                Arrays.asList(rs.getString(2).split(",")));
                    }
                    pst.setString(1, group);
                    pst.setTimestamp(2, fromTs);
                    pst.setTimestamp(3, toTs);
                    try (ResultSet rs2 = pst.executeQuery();) {
                        int channelIndex;
                        String channelName;
                        String devEui;
                        Double d;
                        Timestamp ts;
                        if (logger.isDebugEnabled()) {
                            logger.debug("query: " + query);
                        }
                        String gChnames = "";
                        for (int i = 0; i < groupChannels.size(); i++) {
                            gChnames += groupChannels.get(i) + ",";
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("groupChannels: " + gChnames);
                        }
                        while (rs2.next()) {
                            devEui = rs2.getString(1);
                            ts = rs2.getTimestamp(3);
                            for (int i = 0; i < groupChannels.size(); i++) {
                                channelName = groupChannels.get(i);
                                // logger.debug("channel: " + channelName);
                                channelIndex = devices
                                        .get(devEui)
                                        .indexOf(channelName);
                                if (channelIndex > -1) {
                                    d = rs2.getDouble(4 + channelIndex);
                                    if (rs2.wasNull()) {
                                        d = null;
                                    }
                                } else {
                                    // if device does not have this channel name
                                    d = null;
                                }
                                tmpResult.add(
                                        new ChannelData(
                                                devEui,
                                                channelName,
                                                d,
                                                ts.getTime()));
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                logger.error(e.getMessage());
                throw new IotDatabaseException(
                        IotDatabaseException.SQL_EXCEPTION,
                        e.getMessage(),
                        e);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
                throw new IotDatabaseException(
                        IotDatabaseException.UNKNOWN,
                        ex.getMessage());
            }
            if (tmpResult.isEmpty()) {
                return result;
            }
            String prevEUI = "";
            long prevTimestamp = 0;
            for (int i = 0; i < tmpResult.size(); i++) {
                cd = tmpResult.get(i);
                if (!cd.getDeviceEUI().equalsIgnoreCase(prevEUI)) {
                    if (measuresForEuiTimestamp.size() > 0) {
                        measuresForEui.add(measuresForEuiTimestamp);
                    }
                    if (measuresForEui.size() > 0) {
                        result.add(measuresForEui);
                    }
                    measuresForEui = new ArrayList<>();
                    measuresForEuiTimestamp = new ArrayList<>();
                    prevEUI = cd.getDeviceEUI();
                    prevTimestamp = cd.getTimestamp();
                } else if (prevTimestamp != cd.getTimestamp()) {
                    if (measuresForEuiTimestamp.size() > 0) {
                        measuresForEui.add(measuresForEuiTimestamp);
                    }
                    measuresForEuiTimestamp = new ArrayList<>();
                    prevTimestamp = cd.getTimestamp();
                }
                measuresForEuiTimestamp.add(cd);
            }
            if (measuresForEuiTimestamp.size() > 0) {
                measuresForEui.add(measuresForEuiTimestamp);
            }
            if (measuresForEui.size() > 0) {
                result.add(measuresForEui);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
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
        return result;
    }

    @Override
    public List<IotEvent> getCommands(
            String deviceEui,
            boolean processAll,
            boolean paidOnly) throws IotDatabaseException {
        String query;
        if (null == deviceEui) {
            query = "select id,category,type,origin,payload,createdat from commands order by createdat";
        } else {
            query = "select id,category,type,origin,payload,createdat from commands where origin=? order by createdat";
        }
        List<IotEvent> result = new ArrayList<>();
        Map<String, IotEvent> commands = new HashMap<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            if (null != deviceEui) {
                pst.setString(1, deviceEui);
            }
            try (ResultSet rs = pst.executeQuery();) {
                while (rs.next()) {
                    IotEvent event = new IotEvent();
                    event.setId(rs.getLong(1));
                    event.setCategory(rs.getString(2));
                    event.setType(rs.getString(3));
                    event.setOrigin(rs.getString(4));
                    event.setPayload(rs.getString(5));
                    event.setCreatedAt(rs.getLong(6));
                    if (processAll) {
                        result.add(event);
                    } else {
                        // only first command for each device should be added
                        if (!commands.containsKey(event.getOrigin())) {
                            commands.put(event.getOrigin(), event);
                            result.add(event);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return result;
    }

    @Override
    public IotEvent getFirstCommand(String deviceEUI)
            throws IotDatabaseException {
        String query = "select id,category,type,origin,payload,createdat from commands where origin like ? order by createdat limit 1";
        IotEvent result = null;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, "%@" + deviceEUI);
            // pst.setString(1, deviceEUI);
            try (ResultSet rs = pst.executeQuery();) {
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
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return result;
    }

    /*
     * @Override
     * public long getMaxCommandId() throws IotDatabaseException {
     * String query = "SELECT \n" + //
     * "    MAX(A_max) AS max_id_A,\n" + //
     * "    MAX(B_max) AS max_id_B\n" + //
     * "FROM (\n" + //
     * "    SELECT \n" + //
     * "        MAX(id) AS A_max,\n" + //
     * "        NULL AS B_max\n" + //
     * "    FROM commands\n" + //
     * "    UNION ALL\n" + //
     * "    SELECT \n" + //
     * "        NULL AS A_max,\n" + //
     * "        MAX(id) AS B_max\n" + //
     * "    FROM commandslog\n" + //
     * ") AS max_values;";
     * long result = 0;
     * Long v1 = 0L;
     * Long v2 = 0L;
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pst =
     * conn.prepareStatement(query);) {
     * ResultSet rs = pst.executeQuery();
     * if (rs.next()) {
     * v1 = rs.getLong(1);
     * v1 = rs.getLong(2);
     * }
     * if(null==v1){
     * v1 = 0L;
     * }
     * if(null==v2){
     * v2 = 0L;
     * }
     * if (v1 > v2) {
     * result = v1;
     * } else {
     * result = v2;
     * }
     * rs.close();
     * } catch (SQLException e) {
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION);
     * }
     * return result;
     * }
     */

    /*
     * @Override
     * public long getMaxCommandId(String deviceEui) throws IotDatabaseException {
     * String query =
     * "SELECT  max(commands.id), max(commandslog.id) FROM commands CROSS JOIN commandslog "
     * + "WHERE commands.origin=commandslog.origin AND commands.origin like %@" +
     * deviceEui;
     * long result = 0;
     * long v1 = 0;
     * long v2 = 0;
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pst =
     * conn.prepareStatement(query);) {
     * ResultSet rs = pst.executeQuery();
     * if (rs.next()) {
     * v1 = rs.getLong(1);
     * v1 = rs.getLong(2);
     * }
     * if (v1 > v2) {
     * result = v1;
     * } else {
     * result = v2;
     * }
     * rs.close();
     * } catch (SQLException e) {
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION);
     * }
     * return result;
     * }
     */
    @Override
    public void removeCommand(long id) throws IotDatabaseException {
        String query = "delete from commands where id=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, id);
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public void putVirtualData(Device device, VirtualData data)
            throws IotDatabaseException {
        JsonMapper mapper = new JsonMapper();
        String serialized;
        try {
            serialized = mapper.writeValueAsString(data);
            if (logger.isDebugEnabled()) {
                logger.debug(serialized);
            }
        } catch (JsonProcessingException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.UNKNOWN,
                    "",
                    null);
        }
        String query = "INSERT INTO virtualdevicedata (eui, tstamp, data) VALUES (?,?,?); ";
        // + " ON CONFLICT (eui) DO UPDATE SET tstamp = EXCLUDED.tstamp, data =
        // EXCLUDED.data;";
        // String query = "MERGE INTO virtualdevicedata (eui, tstamp, data) KEY (eui)
        // values (?,?,?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, device.getEUI());
            pst.setTimestamp(2, new Timestamp(data.timestamp));
            pst.setString(3, serialized);
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    @CacheResult(cacheName = "devchannels-cache")
    public int getChannelIndex(String deviceEUI, String channel)
            throws IotDatabaseException {
        return getDeviceChannels(deviceEUI).indexOf(channel) + 1;
    }

    @Override
    public void putCommandLog(String deviceEUI, IotEvent commandEvent)
            throws IotDatabaseException {
        String query = "insert into commandslog (id,category,type,origin,payload,createdat) values (?,?,?,?,?,?);";
        String command = (String) commandEvent.getPayload();
        if (command.startsWith("#") || command.startsWith("&")) {
            command = command.substring(1);
        }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, commandEvent.getId());
            pst.setString(2, commandEvent.getCategory());
            pst.setString(3, commandEvent.getType());
            pst.setString(4, deviceEUI);
            pst.setString(5, command);
            pst.setLong(6, commandEvent.getCreatedAt());
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public void putCommandLog(
            long id,
            String deviceEUI,
            String type,
            String payload,
            long createdAt) throws IotDatabaseException {
        String query = "insert into commandslog (id,category,type,origin,payload,createdat) values (?,?,?,?,?,?);";

        String origin = deviceEUI;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, id);
            pst.setString(2, "");
            pst.setString(3, type);
            pst.setString(4, origin);
            pst.setString(5, payload);
            pst.setLong(6, createdAt);
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public void addAlert(IotEvent event) throws IotDatabaseException {
        String userStr = event
                .getOrigin()
                .substring(0, event.getOrigin().indexOf("\t"));
        String[] users = userStr.split(";");
        String deviceEui = event
                .getOrigin()
                .substring(event.getOrigin().indexOf("\t") + 1);
        String query = "insert into alerts (name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        for (String user : users) {
            if (user.isEmpty()) {
                continue;
            }
            try (
                    Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, "");
                pstmt.setString(2, event.getCategory());
                pstmt.setString(3, event.getType());
                pstmt.setString(4, deviceEui);
                pstmt.setString(5, user);
                pstmt.setString(
                        6,
                        (null != event.getPayload())
                                ? event.getPayload().toString()
                                : "");
                pstmt.setString(7, "");
                pstmt.setString(8, "");
                pstmt.setString(9, "");
                pstmt.setLong(10, 0);
                pstmt.setLong(11, event.getCreatedAt());
                pstmt.setLong(12, -1);
                pstmt.setBoolean(13, false);
                int updated = pstmt.executeUpdate();
                if (updated < 1) {
                    throw new IotDatabaseException(
                            IotDatabaseException.UNKNOWN,
                            "Unable to create notification " + event.getId(),
                            null);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IotDatabaseException(
                        IotDatabaseException.SQL_EXCEPTION,
                        e.getMessage(),
                        e);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IotDatabaseException(
                        IotDatabaseException.UNKNOWN,
                        e.getMessage(),
                        null);
            }
        }
        // Alert alert = new Alert(event);
    }

    @Override
    public void addAlert(
            String type,
            String deviceEui,
            String userId,
            String payload,
            long createdAt) throws IotDatabaseException {
        String query = "insert into alerts (category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic) values (?,?,?,?,?,?,?,?,?,?,?,?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, "IOT");
            pstmt.setString(2, type);
            pstmt.setString(3, deviceEui);
            pstmt.setString(4, userId);
            pstmt.setString(5, payload);
            pstmt.setString(6, "");
            pstmt.setString(7, "");
            pstmt.setString(8, "");
            pstmt.setLong(9, 0);
            pstmt.setLong(10, createdAt);
            pstmt.setLong(11, -1);
            pstmt.setBoolean(12, false);
            int updated = pstmt.executeUpdate();
            if (updated < 1) {
                throw new IotDatabaseException(
                        IotDatabaseException.UNKNOWN,
                        "Unable to create alert",
                        null);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.UNKNOWN,
                    e.getMessage(),
                    null);
        }
    }

    @Override
    public List<Alert> getAlerts(String userID, boolean descending)
            throws IotDatabaseException {
        String query = "select id,name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic from alerts where userid = ? order by id ";
        if (descending) {
            query = query.concat(" desc");
        }
        query = query.concat(" limit ?");
        ArrayList<Alert> list = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setLong(2, requestLimit);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(buildAlert(rs));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return list;
    }

    @Override
    public Long getAlertsCount(String userID) throws IotDatabaseException {
        Long result = 0L;
        String query = "select count(*) from alerts where userid = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    result = rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return result;
    }

    @Override
    public List<Alert> getAlerts(
            String userID,
            int limit,
            int offset,
            boolean descending) throws IotDatabaseException {
        ArrayList<Alert> list = new ArrayList<>();
        String query = "select id,name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic from alerts where userid = ? order by id ";
        if (descending) {
            query = query.concat(" desc");
        }
        query = query.concat(" limit ? offset ?");
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setLong(2, limit);
            pstmt.setLong(3, offset);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(buildAlert(rs));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return list;
    }

    @Override
    public void removeAlert(long alertID) throws IotDatabaseException {
        String query = "delete from alerts where id=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, alertID);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeAlerts(String userID) throws IotDatabaseException {
        String query = "delete from alerts where userid=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeAlerts(long checkpoint) throws IotDatabaseException {
        String query = "delete from alerts where createdat < ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, checkpoint);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void archiveAlerts(long checkpoint) throws IotDatabaseException {
        String query = "INSERT INTO archive_alerts (id, name, category, type, deviceeui, userid, payload, timepoint, serviceid, uuid, calculatedtimepoint, createdat, rooteventid, cyclic) "
                + "SELECT id, name, category, type, deviceeui, userid, payload, timepoint, serviceid, uuid, calculatedtimepoint, to_timestamp(createdat), rooteventid, cyclic "
                + "FROM alerts WHERE createdat < ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, checkpoint / 1000);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
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
                + " d.administrators, d.framecheck, d.configuration, d.organization, d.organizationapp, a.config AS appconfig FROM devices AS d"
                + " LEFT JOIN applications AS a WHERE d.organizationapp=a.id";
        return query;
    }

    @Override
    public void addSmsLog(
            long id,
            boolean confirmed,
            String phone,
            String text) throws IotDatabaseException {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeOutdatedSmsLogs(long checkpoint)
            throws IotDatabaseException {
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
    public void updateDeviceStatus(
            String eui,
            long transmissionInterval,
            Double newStatus,
            int newAlertStatus) throws IotDatabaseException {
        if (logger.isDebugEnabled()) {
            logger.debug("Updating device status.");
        }

        String paidTypes = "(0,1,5,7,8,9,10)";
        String query;
        // if (null != newStatus) {
        // TODO: paid status based on user type
        query = "INSERT INTO devicestatus (eui, paid, tinterval, status, alert) "
                + //
                "VALUES (?, (select type from users where uid=(SELECT userid from devices where eui=?)) in "
                + paidTypes
                + ", ?, ?, ?);";
        // } else {
        // query = "update devices set lastseen=?,lastframe=?,downlink=?,devid=? where
        // eui=?";
        // }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, eui);
            pstmt.setString(2, eui);
            pstmt.setLong(3, transmissionInterval);
            pstmt.setDouble(4, newStatus);
            pstmt.setInt(5, newAlertStatus);
            int updated = pstmt.executeUpdate();
            if (updated < 1) {
                logger.warn("DB error updating device " + eui);
                /*
                 * throw new IotDatabaseException(IotDatabaseException.UNKNOWN,
                 * "DB error updating device " + eui, null);
                 */
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Status rows updated: " + updated);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.UNKNOWN,
                    e.getMessage(),
                    null);
        }
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY account_params to '/var/lib/postgresql/data/export/account_params.csv' DELIMITER ';' CSV HEADER;"
                + "COPY account_features to '/var/lib/postgresql/data/export/account_features.csv' DELIMITER ';' CSV HEADER;"
                + "COPY alerts to '/var/lib/postgresql/data/export/alerts.csv' DELIMITER ';' CSV HEADER;"
                + "COPY archive_alerts to '/var/lib/postgresql/data/export/archive_alerts.csv' DELIMITER ';' CSV HEADER;"
                + "COPY analyticdata to '/var/lib/postgresql/data/export/analyticdata.csv' DELIMITER ';' CSV HEADER;"
                + "COPY commands to '/var/lib/postgresql/data/export/commands.csv' DELIMITER ';' CSV HEADER;"
                + "COPY commandslog to '/var/lib/postgresql/data/export/commandslog.csv' DELIMITER ';' CSV HEADER;"
                + "COPY dashboards to '/var/lib/postgresql/data/export/dashboards.csv' DELIMITER ';' CSV HEADER;"
                + "COPY dashboardtemplates to '/var/lib/postgresql/data/export/dashboardtemplates.csv' DELIMITER ';' CSV HEADER;"
                + "COPY devicechannels to '/var/lib/postgresql/data/export/devicechannels.csv' DELIMITER ';' CSV HEADER;"
                + "COPY devicedata to '/var/lib/postgresql/data/export/devicedata.csv' DELIMITER ';' CSV HEADER;"
                + "COPY devicestatus to '/var/lib/postgresql/data/export/devicestatus.csv' DELIMITER ';' CSV HEADER;"
                + "COPY devices to '/var/lib/postgresql/data/export/devices.csv' DELIMITER ';' CSV HEADER;"
                + "COPY device_tags to '/var/lib/postgresql/data/export/device_tags.csv' DELIMITER ';' CSV HEADER;"
                + "COPY devicetemplates to '/var/lib/postgresql/data/export/devicetemplates.csv' DELIMITER ';' CSV HEADER;"
                + "COPY favourites to '/var/lib/postgresql/data/export/favourites.csv' DELIMITER ';' CSV HEADER;"
                + "COPY groups to '/var/lib/postgresql/data/export/groups.csv' DELIMITER ';' CSV HEADER;"
                + "COPY virtualdevicedata to '/var/lib/postgresql/data/export/virtualdevicedata.csv' DELIMITER ';' CSV HEADER;";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public long putDeviceCommand(
            String deviceEUI,
            String type,
            String payload,
            Long createdAt) throws IotDatabaseException {
        String query = "insert into commands (category,type,origin,payload,createdat) values (?,?,?,?,?);";
        String query2 = "DELETE FROM commands WHERE origin=?; INSERT into commands (category,type,origin,payload,createdat) values (?,?,?,?,?);";
        String command = payload;
        boolean overwrite = false;
        if (command.startsWith("&")) {
            command = command.substring(1);
        } else if (command.startsWith("#")) {
            command = command.substring(1);
            overwrite = true;
            query = query2;
        } else {
            // overwrite = false
            // comand string is not modified
        }

        String origin = deviceEUI;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            if (overwrite) {
                pst.setString(1, origin);
                pst.setString(2, "");
                pst.setString(3, type);
                pst.setString(4, origin);
                pst.setString(5, command);
                pst.setLong(6, createdAt);
            } else {
                pst.setString(1, "");
                pst.setString(2, type);
                pst.setString(3, origin);
                pst.setString(4, command);
                pst.setLong(5, createdAt);
            }
            pst.executeUpdate();
            long id = 0;
            if (!overwrite) {
                try (ResultSet generatedKeys = pst.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        id = generatedKeys.getLong(1);
                    }
                }
            }
            return id;
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public void putDeviceCommand(String deviceEUI, IotEvent commandEvent)
            throws IotDatabaseException {
        String query = "insert into commands (category,type,origin,payload,createdat) values (?,?,?,?,?);";
        String query2 = "DELETE FROM commands WHERE origin=?; INSERT into commands (category,type,origin,payload,createdat) values (?,?,?,?,?);";
        String command = (String) commandEvent.getPayload();
        boolean overwrite = false;
        if (command.startsWith("&")) {
        } else if (command.startsWith("#")) {
            overwrite = true;
            query = query2;
        }
        command = command.substring(1);
        String origin = commandEvent.getOrigin();
        if (null == origin || origin.isEmpty()) {
            origin = deviceEUI;
        }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            if (overwrite) {
                pst.setLong(1, commandEvent.getId());
                pst.setString(2, commandEvent.getCategory());
                pst.setString(3, commandEvent.getType());
                pst.setString(4, origin);
                pst.setString(5, command);
                pst.setLong(6, commandEvent.getCreatedAt());
            } else {
                pst.setString(1, commandEvent.getCategory());
                pst.setString(2, commandEvent.getType());
                pst.setString(3, origin);
                pst.setString(4, command);
                pst.setLong(5, commandEvent.getCreatedAt());
            }
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public void putData(Device device, ArrayList<ChannelData> values)
            throws IotDatabaseException {
        if (values == null || values.isEmpty()) {
            System.out.println("no values");
            return;
        }
        int limit = 24;
        List channelNames = getDeviceChannels(device.getEUI());
        String query = "insert into devicedata (eui,userid,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,project,state,protected) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        long timestamp = values.get(0).getTimestamp();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, device.getEUI());
            pst.setString(2, device.getUserID());
            pst.setTimestamp(3, new java.sql.Timestamp(timestamp));
            for (int i = 1; i <= limit; i++) {
                pst.setNull(i + 3, java.sql.Types.DOUBLE);
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
                            pst.setDouble(
                                    4 + index,
                                    values.get(i - 1).getValue());
                        } catch (NullPointerException e) {
                            pst.setNull(4 + index, Types.DOUBLE);
                        }
                    }
                }
            }
            pst.setString(28, device.getProject());
            pst.setDouble(29, device.getState());
            pst.setBoolean(30, device.isDataProtected());
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public void saveAnalyticData(Device device, ArrayList<ChannelData> values)
            throws IotDatabaseException {
        if (values == null || values.isEmpty()) {
            System.out.println("no values");
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        /*
         * try {
         * String json = mapper.writeValueAsString(values);
         * logger.debug("Values to save: " + json);
         * } catch (JsonProcessingException e) {
         * // TODO Auto-generated catch block
         * e.printStackTrace();
         * }
         */
        int limit = 24;
        List channelNames = getDeviceChannels(device.getEUI());
        String query = "insert into analyticdata (eui,userid,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,project,state,protected,textvalues) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        long timestamp = values.get(0).getTimestamp();
        try (
                Connection conn = analyticDataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, device.getEUI());
            pst.setString(2, device.getUserID());
            pst.setTimestamp(3, new java.sql.Timestamp(timestamp));
            for (int i = 1; i <= limit; i++) {
                pst.setNull(i + 3, java.sql.Types.DOUBLE);
            }
            int index = -1;
            String channelName;
            HashMap<String, String> stringValues = new HashMap<>();
            for (int i = 1; i <= values.size(); i++) {
                channelName = values.get(i - 1).getName();
                index = channelNames.indexOf(channelName);
                if (index < 0) {
                    stringValues.put(
                            channelName,
                            values.get(i - 1).getStringValue());
                } else if (index < limit) {
                    try {
                        pst.setDouble(4 + index, values.get(i - 1).getValue());
                    } catch (NullPointerException e) {
                        pst.setNull(4 + index, Types.DOUBLE);
                    }
                } else {
                    // TODO: send notification to the user?
                    stringValues.put(
                            channelName,
                            values.get(i - 1).getStringValue());
                }
            }
            pst.setString(28, device.getProject());
            pst.setDouble(29, device.getState());
            pst.setBoolean(30, device.isDataProtected());
            try {
                String json = mapper.writeValueAsString(stringValues);
                if (logger.isDebugEnabled()) {
                    logger.debug("String values: " + json);
                }
                // set value of JSONB column
                pst.setObject(31, json, Types.OTHER);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
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
    @CacheResult(cacheName = "device-cache2")
    @Override
    public Device getDevice(
            String deviceEUI,
            boolean withStatus,
            boolean withTags) throws IotDatabaseException {
        Device device = getDevice(deviceEUI, withStatus);
        if (withTags) {
            try {
                List<Tag> tags = getDeviceTags(deviceEUI);
                if (logger.isDebugEnabled()) {
                    logger.debug("Found tags: " + tags.size());
                }
                device.setTags(tags);
            } catch (IotDatabaseException e) {
                logger.error("Error getting device tags: " + e.getMessage());
            }
        }
        return device;
    }

    @CacheResult(cacheName = "device-cache")
    @Override
    public Device getDevice(String deviceEUI, boolean withStatus)
            throws IotDatabaseException {
        if (deviceEUI == null || deviceEUI.isEmpty()) {
            return null;
        }
        // DeviceSelector selector = new DeviceSelector(null, false, withStatus, true,
        // null, null);
        Device device = null;
        DeviceSelector selector = new DeviceSelector(deviceEUI, withStatus);
        String query = selector.query;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    device = buildDevice(rs);
                    // logger.info("Found device: " + device.getEUI()+" using status: " +
                    // device.isStatusUsed()+" needed: " + withStatus);
                    if (withStatus && device.isStatusUsed()) {
                        device = getDeviceStatusData(device);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return device;
    }

    @Override
    public DeviceStatusDto getDeviceStatus(String deviceEUI)
            throws IotDatabaseException {
        String query = "SELECT last(ts,ts) AS lastseen, last(status,ts) AS status, last(alert,ts) AS alert, last(tinterval,ts) AS tinterval, last(paid,ts) as paid FROM devicestatus WHERE eui=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    DeviceStatusDto status = new DeviceStatusDto();
                    status.eui = deviceEUI;
                    status.lastSeen = rs.getTimestamp("lastseen").getTime();
                    status.status = rs.getDouble("status");
                    status.alert = rs.getInt("alert");
                    status.transmissionInterval = rs.getLong("tinterval");
                    status.paid = rs.getBoolean("paid");
                    return status;
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return null;
    }

    public Device getDeviceStatusData(Device device)
            throws IotDatabaseException {
        // String query = "SELECT last(ts,ts) AS lastseen, last(status,ts) AS status,
        // last(alert,ts) AS alert, last(tinterval,ts) AS tinterval FROM devicestatus
        // WHERE eui=?";
        String query = "SELECT last(ts,ts) AS lastseen, last(status,ts) AS status, last(alert,ts) AS alert FROM devicestatus WHERE eui=?";
        logger.info(query);
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, device.getEUI());
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    try {
                        device.setLastSeen(
                                rs.getTimestamp("lastseen").getTime());
                        device.setState(rs.getDouble("status"));
                        device.setAlertStatus(rs.getInt("alert"));
                        // device.setTransmissionInterval(rs.getLong("tinterval"));
                    } catch (Exception e) {
                        // never seen before?
                    }
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return device;
    }

    @CacheResult(cacheName = "devchannels-cache1")
    public LinkedHashMap<String, Integer> getDeviceChannelPositions(
            String deviceEUI) throws IotDatabaseException {
        LinkedHashMap<String, Integer> channels = new LinkedHashMap<String, Integer>();
        String query = "select channels from devicechannels where eui=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    String[] ch = rs.getString(1).toLowerCase().split(",");
                    for (int i = 0; i < ch.length; i++) {
                        channels.put(ch[i], i + 1);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return channels;
    }

    @Override
    public List<String> getDeviceChannels(String deviceEUI)
            throws IotDatabaseException {
        List<String> channels = new ArrayList<>();
        String query = "select channels from devicechannels where eui=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    String[] s = rs.getString(1).toLowerCase().split(",");
                    channels = Arrays.asList(s);
                    String channelStr = "";
                    for (int i = 0; i < channels.size(); i++) {
                        channelStr = channelStr + channels.get(i) + ",";
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                                "CHANNELS READ: " + deviceEUI + " " + channelStr);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return channels;
    }

    @Override
    @CacheResult(cacheName = "values-cache2")
    public List<List> getValues2(
            String userID,
            String deviceEUI,
            String dataQuery) throws IotDatabaseException {
        DataQuery dq;
        try {
            dq = DataQuery.parse(dataQuery);
            if (dq.getChannels().size() == 1
                    && dq.getChannels().get(0).equals("*")) {
                dq.setChannels(getDeviceChannels(deviceEUI));
            }
        } catch (DataQueryException ex) {
            ex.printStackTrace();
            throw new IotDatabaseException(
                    ex.getCode(),
                    "DataQuery " + ex.getMessage());
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
        try (
                Connection conn = dataSource.getConnection();
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

            try (ResultSet rs = pstmt.executeQuery();) {
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
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        Collections.reverse(values);
        return values;
    }

    private ArrayList<String> getColumnSymbols(
            DataQuery dq,
            HashMap<String, Integer> columnPositions) {
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

    private String buildDataQuery(
            String userID,
            String deviceEUI,
            DataQuery dq,
            ArrayList<String> columnSymbols) {
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
        if (logger.isDebugEnabled()) {
            logger.debug("fromTs:" + dq.getFromTs());
            logger.debug("toTs:" + dq.getToTs());
        }
        if (null != dq.getFromTs() && null != dq.getToTs()) {
            query = query.concat(wherePart);
        }
        query = query.concat(orderPart);
        return query;
    }

    @Override
    @CacheResult(cacheName = "values-cache")
    public List<List> getValues(
            String userID,
            String deviceEUI,
            String dataQuery) throws IotDatabaseException {
        if (logger.isDebugEnabled()) {
            logger.debug("queryLimit:" + requestLimit);
            logger.debug("getValues dataQuery:" + dataQuery);
        }
        DataQuery dq;
        try {
            dq = DataQuery.parse(dataQuery);
        } catch (DataQueryException ex) {
            throw new IotDatabaseException(
                    ex.getCode(),
                    "DataQuery " + ex.getMessage());
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
            result.add(
                    getChannelValues(
                            userID,
                            deviceEUI,
                            dq.getChannelName(),
                            limit,
                            dq)); // project
            t1 = System.currentTimeMillis();
            if (logger.isDebugEnabled()) {
                logger.debug("Query time [ms] 1: " + (t1 - t0));
            }
        } else {
            String[] channels = dq.getChannelName().split(",");
            List<ChannelData>[] temp = new ArrayList[channels.length];
            for (int i = 0; i < channels.length; i++) {
                temp[i] = getChannelValues(
                        userID,
                        deviceEUI,
                        channels[i],
                        limit,
                        dq); // project
            }
            t1 = System.currentTimeMillis();
            if (logger.isDebugEnabled()) {
                logger.debug("Query time [ms] 2: " + (t1 - t0));
                logger.debug("DQ limit: " + limit);
            }
            List<ChannelData> values;
            int realLimit = 0;
            for (int j = 0; j < channels.length; j++) {
                if (temp[j].size() > realLimit) {
                    realLimit = temp[j].size();
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Result limit: " + realLimit);
            }
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
            if (logger.isDebugEnabled()) {
                logger.debug("Query processing time [ms]: " + (t2 - t1));
            }
            return result;
        }

        ChannelData data = new ChannelData(
                dq.getChannelName(),
                0.0,
                System.currentTimeMillis());
        data.setNullValue();
        List<ChannelData> subResult = new ArrayList<>();
        Double actualValue = null;
        Double tmpValue;
        int size = 0;
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "DQ: "
                            + dq.average
                            + " "
                            + dq.maximum
                            + " "
                            + dq.minimum
                            + " "
                            + dq.summary);
        }
        if (dq.average > 0) {
            if (result.size() > 0) {
                size = result.get(0).size();
                for (int i = 0; i < size; i++) {
                    if (i == 0) {
                        actualValue = ((ChannelData) result
                                .get(0)
                                .get(i)).getValue();
                    } else {
                        actualValue = actualValue
                                + ((ChannelData) result.get(0).get(i)).getValue();
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
                        actualValue = ((ChannelData) result
                                .get(0)
                                .get(i)).getValue();
                    } else {
                        actualValue = actualValue
                                + ((ChannelData) result.get(0).get(i)).getValue();
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
        if (logger.isDebugEnabled()) {
            logger.debug("Query processing time [ms]: " + (t2 - t1));
        }
        return result;
    }

    private List<List> getValues(
            String userID,
            String deviceEUI,
            int limit,
            DataQuery dataQuery) throws IotDatabaseException {
        String query = SqlQueryBuilder.buildDeviceDataQuery(-1, dataQuery);
        List<String> channels = getDeviceChannels(deviceEUI);
        List<List> result = new ArrayList<>();
        ArrayList<ChannelData> row;
        ArrayList row2;
        // System.out.println("SQL QUERY: " + query);
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
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
                if (logger.isDebugEnabled()) {
                    logger.debug("fromTS: " + dataQuery.getFromTs().getTime());
                }
                pst.setTimestamp(paramIdx, dataQuery.getFromTs());
                paramIdx++;
                if (logger.isDebugEnabled()) {
                    logger.debug("toTS: " + dataQuery.getToTs().getTime());
                }
                pst.setTimestamp(paramIdx, dataQuery.getToTsExclusive());
                paramIdx++;
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("fromTS: " + dataQuery.getFromTs());
                }
            }
            pst.setInt(
                    paramIdx,
                    dataQuery.getLimit() == 0 ? limit : dataQuery.getLimit());

            try (ResultSet rs = pst.executeQuery();) {
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
                                row.add(
                                        new ChannelData(
                                                deviceEUI,
                                                channels.get(i),
                                                d,
                                                rs.getTimestamp(5).getTime()));
                            }
                        }
                        result.add(row);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return result;
    }

    private List<ChannelData> getChannelValues(
            String userID,
            String deviceEUI,
            String channel,
            int resultsLimit,
            DataQuery dataQuery) throws IotDatabaseException {
        ArrayList<ChannelData> result = new ArrayList<>();
        int channelIndex = getChannelIndex(deviceEUI, channel);
        if (channelIndex < 1) {
            return result;
        }
        String query = SqlQueryBuilder.buildDeviceDataQuery(
                channelIndex,
                dataQuery);
        int limit = resultsLimit;
        if (requestLimit > 0 && requestLimit < limit) {
            limit = (int) requestLimit;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("SQL QUERY: " + query);
        }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
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

            try (ResultSet rs = pst.executeQuery();) {
                Double d;
                while (rs.next()) {
                    d = rs.getDouble(6);
                    if (!rs.wasNull()) {
                        result.add(
                                0,
                                new ChannelData(
                                        deviceEUI,
                                        channel,
                                        d,
                                        rs.getTimestamp(5).getTime()));
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("problematic query = " + query);
            e.printStackTrace();
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return result;
    }

    private List<List> getVirtualDeviceMeasures(
            String userID,
            String deviceEUI,
            DataQuery dataQuery) throws IotDatabaseException {
        List<List> result = new ArrayList<>();
        String query = SqlQueryBuilder.buildDeviceDataQuery(-1, dataQuery);
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            try (ResultSet rs = pst.executeQuery();) {
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
                    JsonObject jo = (JsonObject) JsonReader.jsonToJava(
                            serializedData);
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
            }
        } catch (SQLException e) {
            logger.error("problematic query = " + query);
            e.printStackTrace();
            throw new IotDatabaseException(e.getErrorCode(), e.getMessage());
        }
        return result;
    }

    @Override
    public ChannelData getMinimalValue(
            String userID,
            String deviceID,
            String channel,
            int scope,
            Double newValue) throws IotDatabaseException {
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
    public ChannelData getMaximalValue(
            String userID,
            String deviceID,
            String channel,
            int scope,
            Double newValue) throws IotDatabaseException {
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
    public ChannelData getSummaryValue(
            String userID,
            String deviceID,
            String channel,
            int scope,
            Double newValue) throws IotDatabaseException {
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
    public ChannelData getAverageValue(
            String userID,
            String deviceID,
            String channel,
            int scope,
            Double newValue) throws IotDatabaseException {
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

    private ArrayList<Double> getLastValues(
            String deviceEUI,
            String channel,
            int scope) throws IotDatabaseException {
        ArrayList<Double> result = new ArrayList<>();
        int channelIndex = getChannelIndex(deviceEUI, channel);
        if (channelIndex <= 0) {
            return result;
        }
        String columnName = "d" + channelIndex;
        String query = "select "
                + columnName
                + " from analyticdata where eui=? and "
                + columnName
                + " is not null order by tstamp desc limit ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            pst.setInt(2, scope);
            try (ResultSet rs = pst.executeQuery();) {
                while (rs.next()) {
                    result.add(rs.getDouble(1));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return result;
    }

    @Override
    public List<List> getLastValues(String userID, String deviceEUI)
            throws IotDatabaseException {
        String query = "select eui,userid,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24 from analyticdata where eui=? order by tstamp desc limit 1";
        List<String> channels = getDeviceChannels(deviceEUI);
        ArrayList<ChannelData> row = new ArrayList<>();
        ArrayList<List> result = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            try (ResultSet rs = pst.executeQuery();) {
                double d;
                if (rs.next()) {
                    for (int i = 0; i < channels.size(); i++) {
                        d = rs.getDouble(4 + i);
                        if (!rs.wasNull()) {
                            row.add(
                                    new ChannelData(
                                            deviceEUI,
                                            channels.get(i),
                                            d,
                                            rs.getTimestamp(3).getTime()));
                        }
                    }
                    result.add(row);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage(),
                    e);
        }
        return result;
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        if (logger.isDebugEnabled()) {
            logger.debug("createStructure()");
        }
        String query;
        StringBuilder sb;
        // applications
        /*
         * sb = new StringBuilder();
         * sb.append("CREATE TABLE IF NOT EXISTS applications (")
         * .append("id BIGSERIAL primary key,")
         * .append("organization bigint default " + defaultOrganizationId + ",")
         * .append("version bigint default 0,")
         * .append("name varchar UNIQUE,")
         * .append(" configuration varchar);");
         * try (Connection conn = dataSource.getConnection();
         * PreparedStatement pst = conn.prepareStatement(sb.toString());) {
         * pst.executeUpdate();
         * } catch (SQLException e) {
         * e.printStackTrace();
         * logger.error(e.getMessage());
         * }
         * try (Connection conn = dataSource.getConnection();
         * PreparedStatement pst = conn
         * .prepareStatement("INSERT INTO applications values (" + defaultOrganizationId
         * + ","
         * + defaultOrganizationId
         * + ",0,'system','{}');");) {
         * pst.executeUpdate();
         * } catch (SQLException e) {
         *
         * }
         */
        sb = new StringBuilder();
        // devicetemplates
        sb
                .append("CREATE TABLE IF NOT EXISTS devicetemplates (")
                .append("eui varchar primary key,")
                .append("appid varchar,")
                .append("appeui varchar,")
                .append("type varchar,")
                .append("channels varchar,")
                .append("code varchar,")
                .append("decoder varchar,")
                .append("description varchar,")
                .append("tinterval bigint,")
                .append("pattern varchar,")
                .append("commandscript varchar,")
                .append("producer varchar,")
                .append("configuration varchar);");
        // dashboardtemplates
        sb
                .append("CREATE TABLE IF NOT EXISTS dashboardtemplates (")
                .append("id varchar primary key,")
                .append("title varchar,")
                .append("name varchar,")
                .append("items varchar,")
                .append("items_mobile varchar,")
                .append("variables varchar,")
                .append("widgets varchar);");
        // devices
        sb
                .append("CREATE TABLE IF NOT EXISTS devices (")
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
                .append("alert INTEGER,")
                .append("appeui varchar,")
                .append("devid varchar,")
                .append("active boolean,")
                .append("project varchar,")
                .append("latitude double precision,")
                .append("longitude double precision,")
                .append("altitude double precision,")
                .append("state double precision,")
                .append("retention bigint,")
                .append("administrators varchar,")
                .append("framecheck boolean,")
                .append("configuration varchar,")
                .append(
                        "organization bigint default " + defaultOrganizationId + ",")
                .append("organizationapp bigint references applications,")
                .append("defaultdashboard boolean default true,")
                .append("path ltree,")
                .append("status_used boolean default false,")
                .append("createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);");

        // dashboards
        sb
                .append("CREATE TABLE IF NOT EXISTS dashboards (")
                .append("id varchar primary key,")
                .append("name varchar,")
                .append("userid varchar,")
                .append("title varchar,")
                .append("team varchar,")
                .append("widgets varchar,")
                .append("items varchar,")
                .append("token varchar,")
                .append("shared boolean,")
                .append("template varchar,")
                .append("items_mobile varchar,")
                .append("variables varchar,")
                .append(
                        "organization bigint default " + defaultOrganizationId + ",")
                .append("administrators varchar,")
                .append(
                        "created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP);");
        // alerts
        sb
                .append("CREATE TABLE IF NOT EXISTS alerts (")
                .append("id BIGSERIAL primary key ,")
                .append("name varchar,")
                .append("category varchar,")
                .append("type varchar,")
                .append("deviceeui varchar,")
                .append("userid varchar,")
                .append("payload varchar,")
                .append("timepoint varchar,")
                .append("serviceid varchar,")
                .append("uuid varchar,")
                .append("calculatedtimepoint bigint,")
                .append("createdat bigint,")
                .append("rooteventid bigint,")
                .append("cyclic boolean);");
        // devicechannels
        sb
                .append("CREATE TABLE IF NOT EXISTS devicechannels (")
                .append("eui varchar primary key,")
                .append("channels varchar);");
        // devicedata
        sb
                .append("CREATE TABLE IF NOT EXISTS devicedata (")
                .append("eui varchar not null,")
                .append("userid varchar,")
                // .append("day date,")
                // .append("dtime time,")
                .append("tstamp timestamp,")
                .append("d1 double precision,")
                .append("d2 double precision,")
                .append("d3 double precision,")
                .append("d4 double precision,")
                .append("d5 double precision,")
                .append("d6 double precision,")
                .append("d7 double precision,")
                .append("d8 double precision,")
                .append("d9 double precision,")
                .append("d10 double precision,")
                .append("d11 double precision,")
                .append("d12 double precision,")
                .append("d13 double precision,")
                .append("d14 double precision,")
                .append("d15 double precision,")
                .append("d16 double precision,")
                .append("d17 double precision,")
                .append("d18 double precision,")
                .append("d19 double precision,")
                .append("d20 double precision,")
                .append("d21 double precision,")
                .append("d22 double precision,")
                .append("d23 double precision,")
                .append("d24 double precision,")
                .append("project varchar,")
                .append("state double precision,")
                .append("protected boolean default false);");
        sb
                .append("CREATE TABLE IF NOT EXISTS analyticdata (")
                .append("eui text not null,")
                .append("userid text,")
                .append("tstamp timestamptz,")
                .append("d1 double precision,")
                .append("d2 double precision,")
                .append("d3 double precision,")
                .append("d4 double precision,")
                .append("d5 double precision,")
                .append("d6 double precision,")
                .append("d7 double precision,")
                .append("d8 double precision,")
                .append("d9 double precision,")
                .append("d10 double precision,")
                .append("d11 double precision,")
                .append("d12 double precision,")
                .append("d13 double precision,")
                .append("d14 double precision,")
                .append("d15 double precision,")
                .append("d16 double precision,")
                .append("d17 double precision,")
                .append("d18 double precision,")
                .append("d19 double precision,")
                .append("d20 double precision,")
                .append("d21 double precision,")
                .append("d22 double precision,")
                .append("d23 double precision,")
                .append("d24 double precision,")
                .append("project text,")
                .append("state double precision,")
                .append("protected boolean default false,")
                .append("textvalues jsonb);");
        // .append("PRIMARY KEY (eui,tstamp) );");
        // virtualdevicedata
        sb
                .append("CREATE TABLE IF NOT EXISTS virtualdevicedata (")
                .append(
                        "eui TEXT,tstamp TIMESTAMPTZ default current_timestamp, data TEXT, protected BOOLEAN DEFAULT false);");
        // groups
        sb
                .append("CREATE TABLE IF NOT EXISTS groups (")
                .append("eui varchar primary key,")
                .append("name varchar,")
                .append("userid varchar,")
                .append("team varchar,")
                .append("channels varchar,")
                .append("description varchar,")
                .append("administrators varchar,")
                .append(
                        "organization bigint default " + defaultOrganizationId + ");");
        // commands
        sb
                .append("CREATE TABLE IF NOT EXISTS commands (")
                .append("id BIGSERIAL,")
                .append("category varchar,")
                .append("type varchar,")
                .append("origin varchar,")
                .append("payload varchar,")
                .append("createdat bigint,")
                .append("port int,")
                .append("sentat bigint);");
        sb.append(
                "CREATE INDEX IF NOT EXISTS idxcommands on commands(id,origin);");
        // commandslog
        sb
                .append("CREATE TABLE IF NOT EXISTS commandslog (")
                .append("id bigint,")
                .append("category varchar,")
                .append("type varchar,")
                .append("origin varchar,")
                .append("payload varchar,")
                .append("createdat bigint,")
                .append("port int,")
                .append("sentat bigint);");
        sb.append(
                "CREATE INDEX IF NOT EXISTS idxcommandslog on commandslog(id,origin);");
        query = sb.toString();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        /*
         * query =
         * "CREATE INDEX IF NOT EXISTS idx_devicedata_eui_tstamp on devicedata(eui,tstamp);"
         * + "CREATE INDEX IF NOT EXISTS idx_devicedata_tstamp on devicedata(tstamp)";
         * try (Connection conn = dataSource.getConnection(); PreparedStatement pst =
         * conn.prepareStatement(query);) {
         * pst.executeUpdate();
         * } catch (SQLException e) {
         * e.printStackTrace();
         * LOG.error(e.getMessage());
         * }
         */
        // TODO: devicestatus
        query = "CREATE TABLE IF NOT EXISTS devicestatus ( "
                + "eui VARCHAR NOT NULL,"
                + "tinterval BIGINT,"
                + "ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + // lastseen from ts
                "status DOUBLE PRECISION,"
                + "alert INTEGER,"
                + // 0 - unknown, 1 - device ok, 2 - device failure
                "paid BOOLEAN DEFAULT FALSE"
                + ");";
        // + "CREATE INDEX IF NOT EXISTS idx_devicestatus_eui_ts on
        // devicestatus(eui,ts);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        query = "CREATE TABLE IF NOT EXISTS account_params "
                + "(param VARCHAR, accounttype INTEGER, text VARCHAR, value BIGINT, PRIMARY KEY(param,accounttype)); "
                + "CREATE TABLE IF NOT EXISTS account_features "
                + "(feature VARCHAR, accounttype INTEGER, enabled BOOLEAN, PRIMARY KEY(feature,accounttype));";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        query = "CREATE TABLE IF NOT EXISTS favourites ("
                + "userid VARCHAR,"
                + "id VARCHAR,"
                + "is_device BOOLEAN,"
                + // true - device, false - dashboard
                "PRIMARY KEY (userid,id,is_device));";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        query = "CREATE TABLE IF NOT EXISTS device_tags ("
                + "eui TEXT,"
                + "tag_name TEXT,"
                + "tag_value TEXT,"
                + "PRIMARY KEY (eui,tag_name));";

        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        // hypertables
        query = "SELECT create_hypertable('devicedata', 'tstamp');";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }
        query = "SELECT create_hypertable('analyticdata', 'tstamp');";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }

        query = "SELECT create_hypertable('virtualdevicedata', 'tstamp');";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }
        query = "SELECT create_hypertable('devicestatus', 'ts', migrate_data => true);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }

        // TODO: indexes
        // create index devices_userid on devices (userid);
        query = "CREATE INDEX IF NOT EXISTS idx_devicedata_eui_tstamp ON devicedata (eui, tstamp DESC);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.warn(e.getMessage());
        }

        query = "CREATE INDEX IF NOT EXISTS idx_analyticdata_eui_tstamp ON analyticdata (eui, tstamp DESC);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.warn(e.getMessage());
        }
        query = "CREATE INDEX IF NOT EXISTS idx_analyticdata_textvalues ON analyticdata USING GIN (textvalues jsonb_path_ops);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.warn(e.getMessage());
        }

        query = "CREATE INDEX IF NOT EXISTS idx_virtualdevicedata_eui_tstamp ON virtualdevicedata (eui, tstamp DESC);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.warn(e.getMessage());
        }

        query = "CREATE INDEX IF NOT EXISTS idx_devicestatus_eui_ts ON devicestatus (eui, ts DESC);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.warn(e.getMessage());
        }

        query = "CREATE INDEX IF NOT EXISTS idx_alerts_uuid_id ON alerts (uuid, id DESC);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.warn(e.getMessage());
        }

        // alerts archive table
        sb = new StringBuilder();
        sb
                .append("CREATE TABLE IF NOT EXISTS archive_alerts (")
                .append("id BIGINT primary key ,")
                .append("name varchar,")
                .append("category varchar,")
                .append("type varchar,")
                .append("deviceeui varchar,")
                .append("userid varchar,")
                .append("payload varchar,")
                .append("timepoint varchar,")
                .append("serviceid varchar,")
                .append("uuid varchar,")
                .append("calculatedtimepoint bigint,")
                .append("createdat TIMESTAMPTZ NOT NULL,")
                .append("rooteventid bigint,")
                .append("cyclic boolean);");
        query = sb.toString();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        query = "SELECT create_hypertable('archive_alerts', 'createdat',migrate_data => true);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }

        query = "SELECT remove_retention_policy('archive_alerts');";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }
        query = "SELECT add_retention_policy('archive_alerts', INTERVAL '1 year');";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
        }
    }

    /**
     * Get list of devices accessible for user. This method should not be used
     * for users with organization different than default.
     *
     * @param userID    - user ID
     * @param deviceEUI - device EUI
     * @param channel   - channel name
     * @param scope     - scope
     * @param newValue  - new value
     * @return list of device data
     */
    @Override
    public List<Device> getUserDevices(
            User user,
            boolean withStatus,
            Integer limit,
            Integer offset,
            String searchString) throws IotDatabaseException {
        ArrayList<Device> devices = new ArrayList<>();

        if (user.organization != defaultOrganizationId) {
            return devices;
        }
        // TODO: withShared, withStatus
        DeviceSelector selector = new DeviceSelector(
                user,
                false,
                withStatus,
                false,
                limit,
                offset,
                searchString);
        String[] searchParams;
        if (null != searchString) {
            searchParams = searchString.split(":");
        } else {
            searchParams = new String[0];
        }
        String query = selector.query;
        if (logger.isDebugEnabled()) {
            logger.debug("query = " + query);
        }
        Device device;
        String parametrizedParam = "";
        boolean isParametrized = false;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            if (selector.numberOfWritableParams > 0) {
                pst.setString(1, user.uid);
                pst.setString(2, user.uid);
            }
            if (selector.numberOfSearchParams > 0) {
                pst.setString(
                        selector.numberOfWritableParams + 1,
                        "%" + searchParams[1] + "%");
                if (selector.numberOfSearchParams > 1) {
                    parametrizedParam = searchParams[2];
                    if (parametrizedParam.contains("*")) {
                        isParametrized = true;
                        parametrizedParam = parametrizedParam.replace("*", "%");
                    }
                    if (isParametrized) {
                        pst.setString(
                                selector.numberOfWritableParams + 2,
                                parametrizedParam);
                    } else {
                        pst.setString(
                                selector.numberOfWritableParams + 2,
                                "%" + parametrizedParam + "%");
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "parametrizedParam = "
                                    + parametrizedParam
                                    + " at "
                                    + selector.numberOfWritableParams
                                    + 2);
                }
            }
            if (selector.numberOfUserParams > 0) {
                pst.setString(
                        selector.numberOfWritableParams
                                + selector.numberOfSearchParams
                                + 1,
                        user.uid);
                pst.setString(
                        selector.numberOfWritableParams
                                + selector.numberOfSearchParams
                                + 2,
                        "%," + user.uid + ",%");
                pst.setString(
                        selector.numberOfWritableParams
                                + selector.numberOfSearchParams
                                + 3,
                        "%," + user.uid + ",%");
            }
            try (ResultSet rs = pst.executeQuery();) {
                while (rs.next()) {
                    device = buildDevice(rs);
                    if (withStatus) {
                        device = getDeviceStatusData(device);
                    }
                    devices.add(device);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            logger.error(query);
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return devices;
    }

    @Override
    public Integer getUserDevicesCount(String userId)
            throws IotDatabaseException {
        Integer count = 0;
        String query = "SELECT COUNT(*) FROM devices WHERE userid=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, userId);
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    count = rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            logger.error(query);
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return count;
    }

    @Override
    public List<Device> getOrganizationDevices(
            long organizationId,
            boolean withStatus,
            Integer limit,
            Integer offset,
            String searchString) throws IotDatabaseException {
        ArrayList<Device> devices = new ArrayList<>();

        boolean pathSearch = false;
        boolean additionalSearch = false;
        // actual implementation doesn't support path
        // only organization tenants can have devices with path
        String searchCondition = "AND (path IS NULL OR path = '') ";
        String[] searchParts;
        if (null == searchString || searchString.isEmpty()) {
            searchParts = new String[0];
        } else {
            searchParts = searchString.split(":");
            if (searchParts.length == 2) {
                if (searchParts[0].equals("eui")) {
                    searchCondition += "AND eui LIKE ? ";
                } else if (searchParts[0].equals("name")) {
                    searchCondition += "AND name LIKE ? ";
                } else if (searchParts[0].equals("path")) {
                    pathSearch = true;
                    if (searchParts[1].equals("-")) {
                        searchCondition = "AND path is NULL";
                    } else {
                        searchCondition = "AND path ~ ? ";
                    }
                }
            } else if (searchParts.length == 3) {
                if (searchParts[0].equals("tag")) {
                    return getOrganizationDevicesByTag(
                            organizationId,
                            searchParts[1],
                            searchParts[2],
                            limit,
                            offset);
                } else {
                    return devices;
                }
            }
        }
        String query = "SELECT * FROM devices WHERE organization=? "
                + searchCondition
                + " LIMIT ? OFFSET ?";
        Device device;
        int idx = 0;
        if (logger.isDebugEnabled()) {
            logger.debug("query = " + query);
        }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            idx = 2;
            if (searchParts.length > 1) {
                if (pathSearch) {
                    if (searchParts[1].equals("-")) {
                    } else {
                        pst.setObject(2, searchParts[1], Types.OTHER);
                        idx = 3;
                    }
                } else {
                    pst.setString(2, "%" + searchParts[1] + "%");
                    idx = 3;
                }
            }
            pst.setInt(idx, limit);
            pst.setInt(idx + 1, offset);
            try (ResultSet rs = pst.executeQuery();) {
                while (rs.next()) {
                    device = buildDevice(rs);
                    if (withStatus) {
                        device = getDeviceStatusData(device);
                    }
                    devices.add(device);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return devices;
    }

    @Override
    public Device getDevice(
            User user,
            String deviceEUI,
            boolean withShared,
            boolean withStatus) throws IotDatabaseException {
        // TODO: withShared, withStatus
        DeviceSelector selector = new DeviceSelector(
                user,
                withShared,
                withStatus,
                true,
                null,
                null,
                null);
        String query = selector.query;
        Device device = null;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            if (selector.numberOfWritableParams > 0) {
                pst.setString(1, user.uid);
                pst.setString(2, "%," + user.uid + ",%");
            }
            pst.setString(selector.numberOfWritableParams + 1, deviceEUI);
            if (selector.numberOfUserParams > 0) {
                pst.setString(selector.numberOfWritableParams + 2, user.uid);
                pst.setString(
                        selector.numberOfWritableParams + 3,
                        "%," + user.uid + ",%");
                pst.setString(
                        selector.numberOfWritableParams + 4,
                        "%," + user.uid + ",%");
            }
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    device = buildDevice(rs);
                    if (withStatus) {
                        device = getDeviceStatusData(device);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
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
        if (rs.wasNull()) {
            device.setOrgApplicationId(null);
        }
        device.setDashboard(rs.getBoolean("defaultdashboard"));
        try {
            device.setDownlink(rs.getString("downlink"));
        } catch (Exception e) {
            device.setDownlink("");
        }
        try {
            device.setWritable(rs.getBoolean("writable"));
        } catch (Exception e) {
            device.setWritable(true); // writable won't be used in new access logic
        }
        try {
            device.setPath(rs.getString("path"));
        } catch (Exception e) {
            e.printStackTrace();
            device.setPath("");
        }
        try {
            device.setApplicationConfig(rs.getString("appconfig"));
        } catch (Exception e) {
            device.setApplicationConfig("");
        }
        try {
            device.setLastSeen(rs.getTimestamp("lastseen").getTime());
        } catch (Exception e) {
            // device.setLastSeen(0);
        }
        try {
            device.setState(rs.getDouble("status"));
        } catch (Exception e) {
            // device.setState(0.0);
        }
        try {
            device.setAlertStatus(rs.getInt("alert"));
        } catch (Exception e) {
            // device.setAlertStatus(0);
        }
        try {
            device.setCreatedAt(rs.getTimestamp("createdat"));
        } catch (Exception e) {
            e.printStackTrace();
            device.setCreatedAt(new Timestamp(0));
        }
        try {
            device.setStatusUsed(rs.getBoolean("status_used"));
        } catch (Exception e) {
            device.setStatusUsed(false);
        }
        return device;
    }

    @Override
    public void deleteDevice(User user, String deviceEUI)
            throws IotDatabaseException {
        // logger.debug("deleteDevice: " + deviceEUI + " for user: " + user.uid);
        Device device = getDevice(user, deviceEUI, false, false);
        if (!device.isWritable()) {
            throw new IotDatabaseException(
                    IotDatabaseException.CONFLICT,
                    "User is not allowed to update device");
        }
        String query = "DELETE FROM devices WHERE eui=?;";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        removeAllDeviceTags(user, deviceEUI);
    }

    @Override
    public void updateDevice(Device updatedDevice) throws IotDatabaseException {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "updatedDevice: "
                            + updatedDevice.getEUI()
                            + " with path: "
                            + updatedDevice.getPath());
        }
        Device device = getDevice(updatedDevice.getEUI(), true, false);
        String query = "UPDATE devices SET name=?, userid=?, type=?, team=?, channels=?, code=?, "
                + "decoder=?, devicekey=?, description=?, tinterval=?, template=?, pattern=?, "
                + "commandscript=?, appid=?, groups=?, appeui=?, devid=?, active=?, project=?, "
                + "latitude=?, longitude=?, altitude=?, retention=?, administrators=?, "
                + "framecheck=?, configuration=?, organization=?, organizationapp=?, defaultdashboard=?, path=?, status_used=? "
                + "WHERE eui=?;";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
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
                // pst.setLong(28, defaultOrganizationId);
                pst.setNull(28, java.sql.Types.BIGINT);
            }
            pst.setBoolean(29, updatedDevice.isDashboard());
            if (null == updatedDevice.getPath()
                    || updatedDevice.getPath().isEmpty()) {
                pst.setNull(30, java.sql.Types.OTHER);
            } else {
                pst.setObject(
                        30,
                        updatedDevice.getPath(),
                        java.sql.Types.OTHER);
            }
            pst.setBoolean(31, updatedDevice.isStatusUsed());
            pst.setString(32, updatedDevice.getEUI());
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        removeAllDeviceTags(null, updatedDevice.getEUI());
        for (Tag tag : updatedDevice.getTagsAsList()) {
            updateDeviceTag(null, updatedDevice.getEUI(), tag.name, tag.value);
        }
    }

    @Override
    public void updateDevice(User user, Device updatedDevice)
            throws IotDatabaseException {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "updatedDevice: "
                            + updatedDevice.getEUI()
                            + " with path: "
                            + updatedDevice.getPath());
        }
        Device device = getDevice(user, updatedDevice.getEUI(), true, false);
        if (!device.isWritable()) {
            throw new IotDatabaseException(
                    IotDatabaseException.CONFLICT,
                    "User is not allowed to update device");
        }
        String query = "UPDATE devices SET name=?, userid=?, type=?, team=?, channels=?, code=?, "
                + "decoder=?, devicekey=?, description=?, tinterval=?, template=?, pattern=?, "
                + "commandscript=?, appid=?, groups=?, appeui=?, devid=?, active=?, project=?, "
                + "latitude=?, longitude=?, altitude=?, retention=?, administrators=?, "
                + "framecheck=?, configuration=?, organization=?, organizationapp=?, defaultdashboard=?, path=?, status_used=? "
                + "WHERE eui=?;";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
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
                // pst.setLong(28, defaultOrganizationId);
                pst.setNull(28, java.sql.Types.BIGINT);
            }
            pst.setBoolean(29, updatedDevice.isDashboard());
            if (null == updatedDevice.getPath()
                    || updatedDevice.getPath().isEmpty()) {
                pst.setNull(30, java.sql.Types.OTHER);
            } else {
                pst.setObject(
                        30,
                        updatedDevice.getPath(),
                        java.sql.Types.OTHER);
            }
            pst.setBoolean(31, updatedDevice.isStatusUsed());
            pst.setString(32, updatedDevice.getEUI());
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        removeAllDeviceTags(user, updatedDevice.getEUI());
        for (Tag tag : updatedDevice.getTagsAsList()) {
            updateDeviceTag(user, updatedDevice.getEUI(), tag.name, tag.value);
        }
    }

    @Override
    public void changeDeviceEui(String eui, String newEui)
            throws IotDatabaseException {
        String query = "UPDATE devices SET eui=? WHERE eui=?;";
        String query2 = "UPDATE devicechannels SET eui=? WHERE eui=?;";
        String query3 = "UPDATE devicedata SET eui=? WHERE eui=?;";
        String query4 = "UPDATE devicestatus SET eui=? WHERE eui=?;";
        String query5 = "UPDATE virtualdevicedata SET eui=? WHERE eui=?;";
        String query6 = "UPDATE device_tags SET eui=? WHERE eui=?;";
        String query7 = "UPDATE favourites SET id=? WHERE id=? AND is_device=true;";

        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);
                PreparedStatement pst2 = conn.prepareStatement(query2);
                PreparedStatement pst3 = conn.prepareStatement(query3);
                PreparedStatement pst4 = conn.prepareStatement(query4);
                PreparedStatement pst5 = conn.prepareStatement(query5);
                PreparedStatement pst6 = conn.prepareStatement(query6);
                PreparedStatement pst7 = conn.prepareStatement(query7);) {
            conn.setAutoCommit(false);
            pst.setString(1, newEui);
            pst.setString(2, eui);
            pst.executeUpdate();
            pst2.setString(1, newEui);
            pst2.setString(2, eui);
            pst2.executeUpdate();
            pst3.setString(1, newEui);
            pst3.setString(2, eui);
            pst3.executeUpdate();
            pst4.setString(1, newEui);
            pst4.setString(2, eui);
            pst4.executeUpdate();
            pst5.setString(1, newEui);
            pst5.setString(2, eui);
            pst5.executeUpdate();
            pst6.setString(1, newEui);
            pst6.setString(2, eui);
            pst6.executeUpdate();
            pst7.setString(1, newEui);
            pst7.setString(2, eui);
            pst7.executeUpdate();

            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void createDevice(User user, Device device)
            throws IotDatabaseException {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "createDevice: " + device.getEUI() + " for user: " + user.uid);
        }
        String query = "INSERT INTO devices (eui, name, userid, type, team, channels, code, "
                + "decoder, devicekey, description, tinterval, template, pattern, "
                + "commandscript, appid, groups, appeui, devid, active, project, "
                + "latitude, longitude, altitude, retention, administrators, "
                + "framecheck, configuration, organization, organizationapp, defaultdashboard, path, status_used) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement(query);) {
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
                // pst.setLong(29, defaultApplicationId);
                pst.setNull(29, java.sql.Types.BIGINT);
            }
            pst.setBoolean(30, device.isDashboard());
            if (null == device.getPath() || device.getPath().isEmpty()) {
                pst.setNull(31, java.sql.Types.OTHER);
            } else {
                pst.setObject(31, device.getPath(), java.sql.Types.OTHER);
            }
            pst.setBoolean(32, device.isStatusUsed());
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    /*
     * // @Override
     * public List<Device> getInactiveDevices_actual() throws IotDatabaseException {
     * ArrayList<DevStamp> stamps = new ArrayList<DevStamp>();
     * ArrayList<Device> result = new ArrayList<Device>();
     * ArrayList<Device> devices = new ArrayList<Device>();
     * // find active devices with tinterval > 0
     * DeviceSelector selector = new DeviceSelector();
     * String query = selector.query;
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt =
     * conn.prepareStatement(query)) {
     * try (ResultSet rs = pstmt.executeQuery();) {
     * while (rs.next()) {
     * Device device = new Device();
     * device.setEUI(rs.getString("eui"));
     * device.setName(rs.getString("name"));
     * device.setUserID(rs.getString("userid"));
     * device.setType(rs.getString("type"));
     * device.setTeam(rs.getString("team"));
     * device.setChannels(rs.getString("channels"));
     * device.setCode(rs.getString("code"));
     * device.setEncoder(rs.getString("decoder"));
     * device.setKey(rs.getString("devicekey"));
     * device.setDescription(rs.getString("description"));
     * device.setTransmissionInterval(rs.getLong("tinterval"));
     * device.setTemplate(rs.getString("template"));
     * device.setPattern(rs.getString("pattern"));
     * device.setCommandScript(rs.getString("commandscript"));
     * device.setApplicationID(rs.getString("appid"));
     * device.setGroups(rs.getString("groups"));
     * device.setApplicationEUI(rs.getString("appeui"));
     * device.setDeviceID(rs.getString("devid"));
     * device.setActive(rs.getBoolean("active"));
     * device.setProject(rs.getString("project"));
     * device.setLatitude(rs.getDouble("latitude"));
     * device.setLongitude(rs.getDouble("longitude"));
     * device.setAltitude(rs.getDouble("altitude"));
     * device.setRetentionTime(rs.getLong("retention"));
     * device.setAdministrators(rs.getString("administrators"));
     * device.setCheckFrames(rs.getBoolean("framecheck"));
     * device.setConfiguration(rs.getString("configuration"));
     * device.setOrganizationId(rs.getLong("organization"));
     * device.setOrgApplicationId(rs.getLong("organizationapp"));
     * device.setDashboard(rs.getBoolean("defaultdashboard"));
     * devices.add(device);
     * }
     * }
     * } catch (SQLException e) {
     * e.printStackTrace();
     * logger.error(e.getMessage());
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
     * e.getMessage());
     * }
     *
     * String query2 =
     * "SELECT eui,ts,alert FROM device_status WHERE eui = ? ORDER BY ts DESC LIMIT 1"
     * ;
     * for (int i = 0; i < devices.size(); i++) {
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pst =
     * conn.prepareStatement(query2);) {
     * Device device = devices.get(i);
     * pst.setString(1, device.getEUI());
     * try (ResultSet rs = pst.executeQuery();) {
     * if (rs.next()) {
     * DevStamp stamp = new DevStamp();
     * stamp.setEui(rs.getString("eui"));
     * stamp.setTs(rs.getTimestamp("ts"));
     * stamp.setAlert(rs.getInt("alert"));
     * stamps.add(stamp);
     * if (stamp.alert < 2 && stamp.ts.before(
     * new Timestamp(System.currentTimeMillis() - device.getTransmissionInterval() *
     * 1000))) {
     * result.add(device);
     * }
     * }
     * }
     * } catch (SQLException e) {
     * e.printStackTrace();
     * logger.error(e.getMessage());
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
     * e.getMessage());
     * }
     * }
     * return result;
     * }
     */
    @Override
    public List<Device> getDevicesRequiringAlert(boolean paid)
            throws IotDatabaseException {
        ArrayList<Device> list = new ArrayList<>();
        ArrayList<DeviceStub> tmpList = new ArrayList<>();
        String query;
        query = "SELECT eui, last(alert,ts) AS alert,"
                + "last(tinterval,ts)/1000 AS ti,"
                + "(extract(epoch from now())*1000 - extract(epoch from last(ts,ts))*1000)/1000 AS delta,"
                + "last(paid,ts) as paid "
                + "FROM devicestatus "
                + "WHERE ts > now () - INTERVAL '1 day' "
                + "GROUP BY eui ORDER BY eui;";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    DeviceStub device = new DeviceStub();
                    device.eui = (rs.getString("eui"));
                    device.alert = (rs.getInt("alert"));
                    device.ti = (rs.getLong("ti"));
                    device.delta = (rs.getLong("delta"));
                    device.paid = (rs.getBoolean("paid"));
                    tmpList.add(device);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }

        ArrayList<DeviceStub> tmpList2 = tmpList
                .stream()
                .filter(
                        device -> device.paid == paid
                                && device.ti > 0
                                && device.alert < 2
                                && device.delta > 2 * device.ti)
                .collect(Collectors.toCollection(ArrayList::new));
        for (DeviceStub device : tmpList2) {
            Device dev = new Device();
            dev.setEUI(device.eui);
            dev.setAlertStatus(device.alert);
            dev.setTransmissionInterval(device.ti * 1000);
            list.add(dev);
        }
        return list;
    }

    private class DeviceStub {

        String eui;
        int alert;
        long ti;
        long delta;
        boolean paid;
    }

    /*
     * @Override
     * public List<Device> getDevicesRequiringAlert(boolean paid) throws
     * IotDatabaseException {
     * ArrayList<Device> list = new ArrayList<>();
     *
     * String query =
     * "SELECT eui, userid, alert, n, lasts, ti, delta, status, team, administrators "
     * +
     * "FROM ( " +
     * "SELECT s.eui, d.userid, last(s.alert,s.ts) AS alert, extract(epoch from now())*1000 AS n,"
     * +
     * "extract(epoch from last(s.ts,s.ts))*1000 AS lasts," +
     * "last(s.tinterval,s.ts)/1000 AS ti," +
     * "(extract(epoch from now())*1000 - extract(epoch from last(s.ts,s.ts))*1000)/1000 AS delta,"
     * +
     * "last(s.status,s.ts) as status," +
     * "d.team, d.administrators " +
     * "FROM devicestatus AS s " +
     * "LEFT JOIN devices AS d ON (d.eui=s.eui) " +
     * "WHERE d.active=true AND s.paid=? " +
     * "GROUP BY d.eui,s.eui " +
     * ") AS q2 " +
     * "WHERE ti > 0 AND alert < 2 AND delta > 2*ti;";
     *
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt =
     * conn.prepareStatement(query);) {
     * pstmt.setBoolean(1, paid);
     * try (ResultSet rs = pstmt.executeQuery();) {
     * while (rs.next()) {
     * Device device = new Device();
     * device.setEUI(rs.getString("eui"));
     * device.setAlertStatus(rs.getInt("alert"));
     * device.setTransmissionInterval(rs.getLong("ti"));
     * device.setState(rs.getDouble("status"));
     * device.setUserID(rs.getString("userid"));
     * list.add(device);
     * }
     * }
     * } catch (SQLException e) {
     * e.printStackTrace();
     * logger.error(e.getMessage());
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
     * e.getMessage());
     * }
     * return list;
     * }
     */

    /*
     * @Override
     * public List<Device> getInactiveDevices() throws IotDatabaseException {
     * DeviceSelector selector = new DeviceSelector(true);
     * String query = selector.query;
     * Device device;
     * ArrayList<Device> list = new ArrayList<>();
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt =
     * conn.prepareStatement(query);) {
     * try (ResultSet rs = pstmt.executeQuery();) {
     *
     * while (rs.next()) {
     * device = buildDevice(rs);
     * device = getDeviceStatusData(device); // adds lastSeen, status,alert
     * list.add(device);
     * }
     * }
     *
     * } catch (SQLException e) {
     * System.out.println(query);
     * e.printStackTrace();
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
     * e.getMessage());
     * }
     * return list;
     * }
     */
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
    public long getParameterValue(String name, long accountType)
            throws IotDatabaseException {
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "getParameterValue: "
                            + name
                            + " for accountType: "
                            + accountType);
        }
        String query = "SELECT value FROM account_params WHERE param=? AND accounttype=?";
        long result = -1;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    result = rs.getLong("value");
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return result;
    }

    @Override
    public String getParameterTextValue(String name, long accountType)
            throws IotDatabaseException {
        String query = "SELECT text FROM account_params WHERE param=? AND accounttype=?";
        String result = null;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    result = rs.getString("text");
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return result;
    }

    @Override
    public void setParameter(
            String name,
            long accountType,
            long value,
            String text) throws IotDatabaseException {
        String query = "INSERT INTO account_params (param, accounttype, value, text) VALUES (?, ?, ?, ?) "
                + "ON CONFLICT (param,accounttype) DO UPDATE SET value=?,text=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            pstmt.setLong(3, value);
            pstmt.setString(4, text);
            pstmt.setLong(5, value);
            pstmt.setString(6, text);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public boolean isFeatureEnabled(String name, long accountType)
            throws IotDatabaseException {
        String query = "SELECT enabled FROM account_features WHERE feature=? AND accounttype=?";
        boolean result = false;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    result = rs.getBoolean("enabled");
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return result;
    }

    @Override
    public void setFeature(String name, long accountType, boolean enabled)
            throws IotDatabaseException {
        String query = "INSERT INTO account_features (feature, accounttype, enabled) VALUES (?, ?, ?) "
                + "ON CONFLICT (feature,accounttype) DO UPDATE SET enabled=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, name);
            pstmt.setLong(2, accountType);
            pstmt.setBoolean(3, enabled);
            pstmt.setBoolean(4, enabled);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void clearDeviceData(String deviceEUI) throws IotDatabaseException {
        String query = "DELETE FROM devicedata WHERE eui=?; DELETE FROM analyticdata WHERE eui=?;";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            pstmt.setString(2, deviceEUI);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void updateDeviceChannels(String deviceEUI, String channels)
            throws IotDatabaseException {
        String query = "INSERT INTO devicechannels (eui, channels) VALUES (?, ?) "
                + "ON CONFLICT (eui) DO UPDATE SET channels=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            pstmt.setString(2, channels);
            pstmt.setString(3, channels);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void addFavouriteDevice(String userID, String eui)
            throws IotDatabaseException {
        String query = "INSERT INTO favourites (userid, id, is_device) VALUES (?, ?, ?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setString(2, eui);
            pstmt.setBoolean(3, true);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void removeFavouriteDevices(String userID, String eui)
            throws IotDatabaseException {
        String query = "DELETE FROM favourites WHERE userid=? AND id=? AND is_device=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setString(2, eui);
            pstmt.setBoolean(3, true);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public List<Device> getFavouriteDevices(String userID)
            throws IotDatabaseException {
        ArrayList<Device> list = new ArrayList<>();
        String query = "SELECT * FROM devices WHERE eui IN (SELECT id FROM favourites WHERE userid=? AND is_device=?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setBoolean(2, true);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(buildDevice(rs));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<Device> getAllDevices() throws IotDatabaseException {
        ArrayList<Device> list = new ArrayList<>();
        String query = "SELECT * FROM devices";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(buildDevice(rs));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public void addDevice(Device device) throws IotDatabaseException {
        String query = "INSERT INTO devices (eui, name, userid, type, team, channels, code, "
                + "decoder, devicekey, description, tinterval, template, pattern, "
                + "commandscript, appid, groups, appeui, devid, active, project, "
                + "latitude, longitude, altitude, retention, administrators, "
                + "framecheck, configuration, organization, organizationapp, defaultdashboard, path, status_used) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, device.getEUI());
            pstmt.setString(2, device.getName());
            pstmt.setString(3, device.getUserID());
            pstmt.setString(4, device.getType());
            pstmt.setString(5, device.getTeam());
            pstmt.setString(6, device.getChannelsAsString());
            pstmt.setString(7, device.getCode());
            pstmt.setString(8, device.getEncoder());
            pstmt.setString(9, device.getKey());
            pstmt.setString(10, device.getDescription());
            pstmt.setLong(11, device.getTransmissionInterval());
            pstmt.setString(12, device.getTemplate());
            pstmt.setString(13, device.getPattern());
            pstmt.setString(14, device.getCommandScript());
            pstmt.setString(15, device.getApplicationID());
            pstmt.setString(16, device.getGroups());
            pstmt.setString(17, device.getApplicationEUI());
            pstmt.setString(18, device.getDeviceID());
            pstmt.setBoolean(19, device.isActive());
            pstmt.setString(20, device.getProject());
            if (null != device.getLatitude()) {
                pstmt.setDouble(21, device.getLatitude());
            } else {
                pstmt.setNull(21, java.sql.Types.DOUBLE);
            }
            if (null != device.getLongitude()) {
                pstmt.setDouble(22, device.getLongitude());
            } else {
                pstmt.setNull(22, java.sql.Types.DOUBLE);
            }
            if (null != device.getAltitude()) {
                pstmt.setDouble(23, device.getAltitude());
            } else {
                pstmt.setNull(23, java.sql.Types.DOUBLE);
            }
            pstmt.setLong(24, device.getRetentionTime());
            pstmt.setString(25, device.getAdministrators());
            pstmt.setBoolean(26, device.isCheckFrames());
            pstmt.setString(27, device.getConfiguration());
            if (null != device.getOrganizationId()) {
                pstmt.setLong(28, device.getOrganizationId());
            } else {
                pstmt.setLong(28, defaultOrganizationId);
            }
            if (null != device.getOrgApplicationId()) {
                pstmt.setLong(29, device.getOrgApplicationId());
            } else {
                pstmt.setLong(29, defaultApplicationId);
            }
            pstmt.setBoolean(30, device.isDashboard());
            if (null == device.getPath() || device.getPath().isEmpty()) {
                pstmt.setNull(31, java.sql.Types.OTHER);
            } else {
                pstmt.setObject(31, device.getPath(), java.sql.Types.OTHER);
            }
            pstmt.setBoolean(32, device.isStatusUsed());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    /*
     * private String eui; //product type
     * private String appid;
     * private String appeui;
     * private String type;
     * private String channels;
     * private String code;
     * private String decoder;
     * private String description;
     * private int interval;
     * private String pattern; //required fields
     * private String commandScript;
     * private String producer; //producer name
     * private String configuration;
     */
    @Override
    public List<DeviceTemplate> getAllDeviceTemplates()
            throws IotDatabaseException {
        ArrayList<DeviceTemplate> list = new ArrayList<>();
        String query = "SELECT * FROM devicetemplates";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    DeviceTemplate template = new DeviceTemplate();
                    template.setEui(rs.getString("eui"));
                    template.setAppid(rs.getString("appid"));
                    template.setAppeui(rs.getString("appeui"));
                    template.setDecoder(rs.getString("decoder"));
                    template.setCode((rs.getString("code")));
                    template.setPattern(rs.getString("pattern"));
                    template.setCommandScript(rs.getString("commandscript"));
                    template.setConfiguration(rs.getString("configuration"));
                    template.setChannels(rs.getString("channels"));
                    template.setDescription(rs.getString("description"));
                    template.setInterval(rs.getInt("tinterval"));
                    template.setPattern(rs.getString("pattern"));
                    template.setProducer(rs.getString("producer"));
                    template.setType(rs.getString("type"));
                    list.add(template);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public void addDeviceTemplate(DeviceTemplate device)
            throws IotDatabaseException {
        String query = "INSERT INTO devicetemplates (eui, appid, appeui, type, channels, code, "
                + "decoder, description, tinterval, pattern, commandscript, producer, configuration) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, device.getEui());
            pstmt.setString(2, device.getAppid());
            pstmt.setString(3, device.getAppeui());
            pstmt.setString(4, device.getType());
            pstmt.setString(5, device.getChannels());
            pstmt.setString(6, device.getCode());
            pstmt.setString(7, device.getDecoder());
            pstmt.setString(8, device.getDescription());
            pstmt.setInt(9, device.getInterval());
            pstmt.setString(10, device.getPattern());
            pstmt.setString(11, device.getCommandScript());
            pstmt.setString(12, device.getProducer());
            pstmt.setString(13, device.getConfiguration());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public DeviceGroup getGroup(String groupEUI) throws IotDatabaseException {
        DeviceGroup group = null;
        // logger.debug("getGroup: " + groupEUI);
        String query = "SELECT * FROM groups WHERE eui=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, groupEUI);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    group = new DeviceGroup();
                    group.setEUI(rs.getString("eui"));
                    group.setName(rs.getString("name"));
                    group.setUserID(rs.getString("userid"));
                    group.setAdministrators(rs.getString("administrators"));
                    group.setChannelsAsString(rs.getString("channels"));
                    group.setDescription(rs.getString("description"));
                    group.setOrganization(rs.getLong("organization"));
                    group.setTeam(rs.getString("team"));
                    // logger.debug("found "+group.getEUI()+" "+group.getName()+"
                    // "+group.getUserID()+" "+group.getAdministrators()+"
                    // "+group.getChannelsAsString()+" "+group.getDescription()+"
                    // "+group.getOrganization()+" "+group.getTeam());

                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return group;
    }

    @Override
    public List<DeviceGroup> getOrganizationGroups(
            long organizationId,
            int limit,
            int offset,
            String searchString) throws IotDatabaseException {
        String[] searchParts = new String[0];
        String searchCondition = "";
        if (null != searchString && !searchString.isEmpty()) {
            searchParts = searchString.split(":");
            if (searchParts.length == 2) {
                if (searchParts[0].equals("eui")) {
                    searchCondition = "AND eui LIKE ? ";
                } else if (searchParts[0].equals("name")) {
                    searchCondition = "AND name LIKE ? ";
                }
            }
        } else {
        }
        String query = "SELECT * FROM groups WHERE organization=? "
                + searchCondition
                + " LIMIT ? OFFSET ?";
        ArrayList<DeviceGroup> list = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, organizationId);
            if (searchCondition.isEmpty()) {
                pstmt.setInt(2, limit);
                pstmt.setInt(3, offset);
            } else {
                pstmt.setString(2, "%" + searchParts[1] + "%");
                pstmt.setInt(3, limit);
                pstmt.setInt(4, offset);
            }
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    DeviceGroup group = new DeviceGroup();
                    group.setEUI(rs.getString("eui"));
                    group.setName(rs.getString("name"));
                    group.setUserID(rs.getString("userid"));
                    group.setAdministrators(rs.getString("administrators"));
                    group.setChannelsAsString(rs.getString("channels"));
                    group.setDescription(rs.getString("description"));
                    group.setOrganization(rs.getLong("organization"));
                    group.setTeam(rs.getString("team"));
                    list.add(group);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<DeviceGroup> getUserGroups(
            String userID,
            int limit,
            int offset,
            String searchString) throws IotDatabaseException {
        String[] searchParts = new String[0];
        String searchCondition = "";
        if (null != searchString && !searchString.isEmpty()) {
            searchParts = searchString.split(":");
            if (searchParts.length == 2) {
                if (searchParts[0].equals("eui")) {
                    searchCondition = "AND eui LIKE ? ";
                } else if (searchParts[0].equals("name")) {
                    searchCondition = "AND name LIKE ? ";
                }
            }
        }
        String query = "SELECT * FROM groups WHERE userid=? "
                + searchCondition
                + " LIMIT ? OFFSET ?";
        // logger.debug(query);
        // logger.debug(userID+" "+limit+" "+offset);
        ArrayList<DeviceGroup> list = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            if (searchCondition.isEmpty()) {
                pstmt.setInt(2, limit);
                pstmt.setInt(3, offset);
            } else {
                pstmt.setString(2, "%" + searchParts[1] + "%");
                pstmt.setInt(3, limit);
                pstmt.setInt(4, offset);
            }
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    DeviceGroup group = new DeviceGroup();
                    group.setEUI(rs.getString("eui"));
                    group.setName(rs.getString("name"));
                    group.setUserID(rs.getString("userid"));
                    group.setAdministrators(rs.getString("administrators"));
                    group.setChannelsAsString(rs.getString("channels"));
                    group.setDescription(rs.getString("description"));
                    group.setOrganization(rs.getLong("organization"));
                    group.setTeam(rs.getString("team"));
                    list.add(group);
                }
            }
            // logger.debug("getUserGroups: " + list.size());

        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public void updateGroup(DeviceGroup group) throws IotDatabaseException {
        String query = "UPDATE groups SET name=?, userid=?, administrators=?, channels=?, "
                + "description=?, organization=?, team=? WHERE eui=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, group.getName());
            pstmt.setString(2, group.getUserID());
            pstmt.setString(3, group.getAdministrators());
            pstmt.setString(4, group.getChannelsAsString());
            pstmt.setString(5, group.getDescription());
            pstmt.setLong(6, group.getOrganization());
            pstmt.setString(7, group.getTeam());
            pstmt.setString(8, group.getEUI());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void createGroup(DeviceGroup group) throws IotDatabaseException {
        String query = "INSERT INTO groups (eui, name, userid, administrators, channels, "
                + "description, organization, team) VALUES (?,?,?,?,?,?,?,?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, group.getEUI());
            pstmt.setString(2, group.getName());
            pstmt.setString(3, group.getUserID());
            pstmt.setString(4, group.getAdministrators());
            pstmt.setString(5, group.getChannelsAsString());
            pstmt.setString(6, group.getDescription());
            pstmt.setLong(7, group.getOrganization());
            pstmt.setString(8, group.getTeam());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void deleteGroup(String groupEUI) throws IotDatabaseException {
        String query = "DELETE FROM groups WHERE eui=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, groupEUI);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public List<Device> getGroupDevices(
            String userID,
            long organizationID,
            String groupID) throws IotDatabaseException {
        ArrayList<Device> list = new ArrayList<>();
        String query;
        if (organizationID != defaultOrganizationId) {
            query = "SELECT * FROM devices WHERE groups LIKE ? AND organization=?";
        } else {
            query = "SELECT * FROM devices WHERE groups LIKE ? AND userid=?";
        }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, "%," + groupID + ",%");
            if (organizationID != defaultOrganizationId) {
                pstmt.setLong(2, organizationID);
            } else {
                pstmt.setString(2, userID);
            }
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    Device device = buildDevice(rs);
                    list.add(device);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<Device> getGroupDevices(String groupID)
            throws IotDatabaseException {
        ArrayList<Device> list = new ArrayList<>();
        String query = "SELECT * FROM devices WHERE groups LIKE ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, "%," + groupID + ",%");
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    Device device = buildDevice(rs);
                    list.add(device);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<Device> getGroupVirtualDevices(String groupID)
            throws IotDatabaseException {
        ArrayList<Device> list = new ArrayList<>();
        String query = "SELECT * FROM devices WHERE groups LIKE ? and type='VIRTUAL'";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, "%," + groupID + ",%");
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    Device device = buildDevice(rs);
                    list.add(device);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<Tag> getDeviceTags(String deviceEui)
            throws IotDatabaseException {
        ArrayList<Tag> list = new ArrayList<>();
        String query = "SELECT * FROM device_tags WHERE eui=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    Tag tag = new Tag();
                    tag.name = (rs.getString("tag_name"));
                    tag.value = (rs.getString("tag_value"));
                    list.add(tag);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return null;
        }
        return list;
    }

    @Override
    public String getDeviceTagValue(String deviceEui, String tagName)
            throws IotDatabaseException {
        String query = "SELECT tag_value FROM device_tags WHERE eui=? AND tag_name=?";
        String tagValue = null;
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
            pstmt.setString(2, tagName);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    tagValue = rs.getString("tag_value");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return tagValue;
    }

    @Override
    public void addDeviceTag(
            User user,
            String deviceEui,
            String tagName,
            String tagValue) throws IotDatabaseException {
        String query = "INSERT INTO device_tags (eui, tag_name, tag_value) VALUES (?, ?, ?)";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
            pstmt.setString(2, tagName);
            pstmt.setString(3, tagValue);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void removeDeviceTag(User user, String deviceEui, String tagName)
            throws IotDatabaseException {
        String query = "DELETE FROM device_tags WHERE eui=? AND tag_name=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
            pstmt.setString(2, tagName);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void updateDeviceTag(
            User user,
            String deviceEui,
            String tagName,
            String tagValue) throws IotDatabaseException {
        String query = "UPDATE device_tags SET tag_value=? WHERE eui=? AND tag_name=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, tagValue);
            pstmt.setString(2, deviceEui);
            pstmt.setString(3, tagName);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public void removeAllDeviceTags(User user, String deviceEui)
            throws IotDatabaseException {
        String query = "DELETE FROM device_tags WHERE eui=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
    }

    @Override
    public List<Device> getUserDevicesByTag(
            User user,
            String tagName,
            String tagValue,
            Integer limit,
            Integer offset) throws IotDatabaseException {
        String searchValue = tagValue;
        boolean isLikeQuery = false;
        if (tagValue.contains("*")) {
            searchValue = tagValue.replace("*", "%");
            isLikeQuery = true;
        }
        String query = "SELECT * FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE LOWER(tag_name)=LOWER(?) AND LOWER(tag_value)=LOWER(?)) AND userid=? ORDER BY name LIMIT=? OFFSET=?";
        if (isLikeQuery) {
            query = "SELECT * FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE LOWER(tag_name)=LOWER(?) AND LOWER(tag_value) LIKE LOWER(?)) AND userid=? ORDER BY name LIMIT=? OFFSET=?";
        }
        ArrayList<Device> list = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, tagName);
            pstmt.setString(2, searchValue);
            pstmt.setString(3, user.uid);
            pstmt.setInt(4, limit);
            pstmt.setInt(5, offset);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(buildDevice(rs));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<Device> getOrganizationDevicesByTag(
            long organizationId,
            String tagName,
            String tagValue,
            Integer limit,
            Integer offset) throws IotDatabaseException {
        String query = "SELECT * FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE tag_name=? AND tag_value=?) AND organization=? ORDER BY name LIMIT ? OFFSET ?";
        ArrayList<Device> list = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, tagName);
            pstmt.setString(2, tagValue);
            pstmt.setLong(3, organizationId);
            pstmt.setInt(4, limit);
            pstmt.setInt(5, offset);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(buildDevice(rs));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<String> getUserDeviceEuisByTag(
            User user,
            String tagName,
            String tagValue) {
        String query = "SELECT eui FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE tag_name=? AND tag_value=?) AND userid=?";
        ArrayList<String> list = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, tagName);
            pstmt.setString(2, tagValue);
            pstmt.setString(3, user.uid);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(rs.getString("eui"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return null;
        }
        return list;
    }

    @Override
    public List<String> getOrganizationDeviceEuisByTag(
            long organizationId,
            String tagName,
            String tagValue) {
        ArrayList<String> list = new ArrayList<>();
        String query = "SELECT eui FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE tag_name=? AND tag_value=?) AND organization=?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, tagName);
            pstmt.setString(2, tagValue);
            pstmt.setLong(3, organizationId);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(rs.getString("eui"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return null;
        }
        return list;
    }

    @Override
    public List<Device> getDevicesByTag(
            String userID,
            long organizationID,
            String tagName,
            String tagValue) throws IotDatabaseException {
        boolean isLikeQuery = false;
        if (tagValue.contains("*")) {
            tagValue = tagValue.replace("*", "%");
            isLikeQuery = true;
        }
        String query;
        if (organizationID == defaultOrganizationId) {
            if (isLikeQuery) {
                query = "SELECT * FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE tag_name=? AND tag_value LIKE ?) and userid=?";
            } else {
                query = "SELECT * FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE tag_name=? AND tag_value=?) and userid=?";
            }
        } else {
            if (isLikeQuery) {
                query = "SELECT * FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE tag_name=? AND tag_value LIKE ?) and organization=?";
            } else {
                query = "SELECT * FROM devices WHERE eui IN (SELECT eui FROM device_tags WHERE tag_name=? AND tag_value=?) and organization=?";
            }
        }
        ArrayList<Device> list = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, tagName);
            pstmt.setString(2, tagValue);
            if (organizationID == defaultOrganizationId) {
                pstmt.setString(3, userID);
            } else {
                pstmt.setLong(3, organizationID);
            }
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(buildDevice(rs));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<Device> getDevicesByPath(
            String userID,
            long organizationID,
            int tenantId,
            String path,
            String search,
            Integer limit,
            Integer offset) throws IotDatabaseException {
        // TODO: search param:
        // 1. search by path
        // 2. search by name
        // 3. search by eui
        // 4. search by tag
        String query;
        String searchPath = mergePaths(path, search);
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "getDevicesByPath: "
                            + userID
                            + " "
                            + organizationID
                            + " "
                            + path
                            + " "
                            + limit
                            + " "
                            + offset);
        }

        if (organizationID == defaultOrganizationId) {
            query = "SELECT * FROM devices WHERE userid=?";
        } else {
            query = "SELECT * FROM devices WHERE organization=?";
        }
        if (searchPath != null && !searchPath.isEmpty()) {
            query += " AND path ~ ?";
        } else {
            query += " AND path IS NULL";
        }
        if (limit != null && offset != null) {
            query += " LIMIT ? OFFSET ?";
        }
        ArrayList<Device> list = new ArrayList<>();
        if (logger.isDebugEnabled()) {
            logger.debug(query);
            logger.debug(
                    userID
                            + " "
                            + organizationID
                            + " "
                            + searchPath
                            + " "
                            + search
                            + " "
                            + limit
                            + " "
                            + offset);
        }
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            int idx = 2;
            if (organizationID == defaultOrganizationId) {
                pstmt.setString(1, userID);
            } else {
                pstmt.setLong(1, organizationID);
            }
            if (searchPath != null && !searchPath.isEmpty()) {
                pstmt.setObject(2, searchPath, java.sql.Types.OTHER);
                idx = 3;
            }

            if (limit != null && offset != null) {
                pstmt.setInt(idx, limit);
                pstmt.setInt(idx + 1, offset);
            }

            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    list.add(buildDevice(rs));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    /**
     * Merges two paths and returns the result.
     *
     * @param pathOfAccessRights the user has access to.
     * @param searchPath         The path to search for.
     * @return The merged path.
     */
    private String mergePaths(String path, String search) {
        String pathOfAccessRights = path;
        String searchPath = search;
        /*
         * if(pathOfAccessRights!=null && pathOfAccessRights.endsWith(".ALL")){
         * pathOfAccessRights=pathOfAccessRights.substring(0,
         * pathOfAccessRights.length()-4)+".*";
         * }
         * if(searchPath!=null && searchPath.endsWith(".ALL")){
         * searchPath=searchPath.substring(0, searchPath.length()-4)+".*";
         * }
         */
        // name path means pathOfAccessRights
        if (searchPath == null || searchPath.isEmpty()) {
            return pathOfAccessRights;
        }
        String[] parts = searchPath.split(":");
        if (parts.length < 2) {
            return pathOfAccessRights;
        }
        String pathToSearch = null;
        if (parts[0].equals("path")) {
            pathToSearch = parts[1];
        }
        if (pathOfAccessRights == null || pathOfAccessRights.isEmpty()) {
            return pathToSearch;
        }

        String result = null;
        // adding "technical" dot at the end of path, to make it easier to compare paths
        if (!(pathOfAccessRights.endsWith(".*")
                || pathOfAccessRights.endsWith("."))) {
            pathOfAccessRights += ".";
        }
        if (!(pathToSearch.endsWith(".*") || pathToSearch.endsWith("."))) {
            pathToSearch += ".";
        }
        // if path the user has access to is higher (in organization structure tree) or
        // the same as the path to search and ends with *, return the path to search
        // if path the user has access to is higher or the same as the path to search
        // and does not end with *, return the path only if it is exactly the same as
        // the path to search
        if (pathToSearch.startsWith(pathOfAccessRights)) {
            if (pathOfAccessRights.endsWith(".*")) {
                result = pathToSearch;
            } else if (pathOfAccessRights.equals(pathToSearch)) {
                result = pathOfAccessRights;
            } else {
                result = "";
            }
        }
        // if path the user has access to is lower than the path to search and ends with
        // *, return the path
        // if path the user has access to is lower than the path to search and does not
        // end with *, return nothing
        if (pathOfAccessRights.startsWith(pathToSearch)) {
            if (pathToSearch.endsWith(".*")) {
                result = pathOfAccessRights;
            } else {
                result = "";
            }
        }
        // remove last "technical" dot
        if (result.endsWith(".")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    @Override
    public void commit() {
        String query = "COMMIT";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            // throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
            // e.getMessage());
        }
    }

    @Override
    public List<CommandDto> getDeviceCommands(String deviceEui, boolean sent)
            throws IotDatabaseException {
        String query;
        if (sent) {
            query = "SELECT id,category,type,origin,payload,createdat,port,sentat FROM commandslog WHERE origin=?";
        } else {
            query = "SELECT id,category,type,origin,payload,createdat,port,sentat FROM commands WHERE origin=?";
        }
        ArrayList<CommandDto> list = new ArrayList<>();
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEui);
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    CommandDto command = new CommandDto();
                    command.id = rs.getLong("id");
                    command.category = rs.getString("category");
                    command.type = rs.getString("type");
                    command.origin = rs.getString("origin");
                    command.payload = rs.getString("payload");
                    command.port = rs.getInt("port");
                    command.createdAt = rs.getLong("createdat");
                    command.sentAt = rs.getLong("sentat");
                    if (rs.wasNull()) {
                        command.sentAt = null;
                    }
                    list.add(command);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(
                    IotDatabaseException.SQL_EXCEPTION,
                    e.getMessage());
        }
        return list;
    }

    @Override
    public List<CommandDto> getAllCommands(
            String user,
            Long organizationId,
            boolean sent) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException(
                "Unimplemented method 'getAllCommands'");
    }
}
