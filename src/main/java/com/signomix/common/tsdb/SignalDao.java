package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.SignalDaoIface;
import com.signomix.common.iot.sentinel.Signal;

import io.agroal.api.AgroalDataSource;


public class SignalDao implements SignalDaoIface {

    public static final long DEFAULT_ORGANIZATION_ID = 1;

    private static final Logger logger = Logger.getLogger(SignalDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource ds) {
        this.dataSource = ds;
    }

    @Override
    public AgroalDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY (SELECT * FROM signals) to '/var/lib/postgresql/data/export/signals.csv' DELIMITER ';' CSV HEADER;"
                + "COPY (SELECT* FROM user_signals) to '/var/lib/postgresql/data/export/user_signals.csv' DELIMITER ';' CSV HEADER;";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    /**
     * Creates the database structure for Signal objects.
     */
    @Override
    public void createStructure() throws IotDatabaseException {
        String query = "CREATE TABLE IF NOT EXISTS signals ("
                + "id BIGSERIAL, "
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "read_at TIMESTAMPTZ,"
                + "sent_at TIMESTAMPTZ,"
                + "delivered_at TIMESTAMPTZ,"
                + "user_id VARCHAR(255),"
                + "organization_id BIGINT,"
                + "sentinel_config_id BIGINT,"
                + "device_eui VARCHAR(255),"
                + "level INTEGER NOT NULL,"
                + "message_en VARCHAR(255),"
                + "message_pl VARCHAR(255)"
                + ");";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

         query = "CREATE TABLE IF NOT EXISTS user_signals ("
                + "id BIGSERIAL, "
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "read_at TIMESTAMPTZ,"
                + "sent_at TIMESTAMPTZ,"
                + "delivered_at TIMESTAMPTZ,"
                + "user_id VARCHAR(255),"
                + "organization_id BIGINT,"
                + "sentinel_config_id BIGINT,"
                + "device_eui VARCHAR(255),"
                + "level INTEGER NOT NULL,"
                + "message_en VARCHAR(255),"
                + "message_pl VARCHAR(255)"
                + ");";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

        // hypertables
        query = "SELECT create_hypertable('signals', 'created_at',migrate_data => true);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }
        query = "SELECT create_hypertable('user_signals', 'created_at',migrate_data => true);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.execute();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        } 

        // indexes
        query = "CREATE INDEX IF NOT EXISTS signals_org_created_idx ON signals(organization_id, created_at);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }
        query = "CREATE INDEX IF NOT EXISTS usersignals_user_created_idx ON signals(user_id, created_at);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }
        query = "CREATE INDEX IF NOT EXISTS usersignals_org_created_idx ON signals(organization_id, created_at);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.warn(e.getMessage());
        }

    }

    @Override
    public void saveSignal(Signal signal) throws IotDatabaseException {
        String query = "INSERT INTO user_signals (user_id, organization_id, sentinel_config_id, device_eui, level, message_en, message_pl) VALUES (?,?,?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, signal.userId);
            pstmt.setLong(2, signal.organizationId);
            pstmt.setLong(3, signal.sentinelConfigId);
            pstmt.setString(4, signal.deviceEui);
            pstmt.setInt(5, signal.level);
            pstmt.setString(6, signal.messageEn);
            pstmt.setString(7, signal.messagePl);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public Signal getSignalById(long id) throws IotDatabaseException {
        String query = "SELECT * FROM user_signals WHERE id=?";
        Signal signal = null;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            if(rs.next()){
                signal = new Signal();
                signal.id = rs.getLong("id");
                signal.createdAt = rs.getTimestamp("created_at");
                signal.readAt = rs.getTimestamp("read_at");
                signal.sentAt = rs.getTimestamp("sent_at");
                signal.deliveredAt = rs.getTimestamp("delivered_at");
                signal.userId = rs.getString("user_id");
                signal.organizationId = rs.getLong("organization_id");
                signal.sentinelConfigId = rs.getLong("sentinel_config_id");
                signal.deviceEui = rs.getString("device_eui");
                signal.level = rs.getInt("level");
                signal.messageEn = rs.getString("message_en");
                signal.messagePl = rs.getString("message_pl");
                
            }
            rs.close();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return signal;
    }

    @Override
    public void updateSignal(Signal signal) throws IotDatabaseException {
        String query = "UPDATE user_signals SET read_at=?, sent_at=?, delivered_at=? WHERE id=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
                    //TODO: null timestamps
            pstmt.setTimestamp(1, signal.readAt);
            pstmt.setTimestamp(2, signal.sentAt);
            pstmt.setTimestamp(3, signal.deliveredAt);
            pstmt.setLong(4, signal.id);
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    public void deleteSignal(long id) throws IotDatabaseException {
        String query = "DELETE FROM user_signals WHERE id=?";
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
    public List<Signal> getUserSignals(String userId, int limit, int offset) throws IotDatabaseException {
        String query = "SELECT * FROM user_signals WHERE user_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?";
        logger.debug(query);
        logger.debug("userId: "+userId); 
        ArrayList<Signal> signals = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userId);
            pstmt.setInt(2, limit);
            pstmt.setInt(3, offset);
            ResultSet rs = pstmt.executeQuery();
            
            while(rs.next()){
                Signal signal = new Signal();
                signal.id = rs.getLong("id");
                signal.createdAt = rs.getTimestamp("created_at");
                signal.readAt = rs.getTimestamp("read_at");
                signal.sentAt = rs.getTimestamp("sent_at");
                signal.deliveredAt = rs.getTimestamp("delivered_at");
                signal.userId = rs.getString("user_id");
                signal.organizationId = rs.getLong("organization_id");
                signal.sentinelConfigId = rs.getLong("sentinel_config_id");
                signal.deviceEui = rs.getString("device_eui");
                signal.level = rs.getInt("level");
                signal.messageEn = rs.getString("message_en");
                signal.messagePl = rs.getString("message_pl");
                signals.add(signal);
            }
            logger.debug("found signals: "+signals.size());
            rs.close();
            
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return signals;
    }

    @Override
    public List<Signal> getOrganizationSignals(long organizationId, int limit, int offset) throws IotDatabaseException {
        String query = "SELECT * FROM user_signals WHERE organization_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?";
        ArrayList<Signal> signals = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, organizationId);
            pstmt.setInt(2, limit);
            pstmt.setInt(3, offset);
            ResultSet rs = pstmt.executeQuery();
            
            while(rs.next()){
                Signal signal = new Signal();
                signal.id = rs.getLong("id");
                signal.createdAt = rs.getTimestamp("created_at");
                signal.readAt = rs.getTimestamp("read_at");
                signal.sentAt = rs.getTimestamp("sent_at");
                signal.deliveredAt = rs.getTimestamp("delivered_at");
                signal.userId = rs.getString("user_id");
                signal.organizationId = rs.getLong("organization_id");
                signal.sentinelConfigId = rs.getLong("sentinel_config_id");
                signal.deviceEui = rs.getString("device_eui");
                signal.level = rs.getInt("level");
                signal.messageEn = rs.getString("message_en");
                signal.messagePl = rs.getString("message_pl");
                signals.add(signal);
            }
            rs.close();
            
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
        return signals;
    }

}
