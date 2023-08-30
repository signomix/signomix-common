package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Singleton;

import org.jboss.logging.Logger;

import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;
import io.quarkus.cache.CacheResult;

@Singleton
public class AuthDao implements AuthDaoIface {
    private static final Logger LOG = Logger.getLogger(AuthDao.class);

    String permanentTokenPrefix = "~~";

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        String query = "CREATE TABLE IF NOT EXISTS tokens (token varchar primary key, uid varchar, issuer varchar, payload varchar, tstamp timestamp, eoflife timestamp);"
                + "CREATE TABLE IF NOT EXISTS ptokens (token varchar primary key, uid varchar, issuer varchar, payload varchar, tstamp timestamp, eoflife timestamp);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    @Override
    @CacheResult(cacheName = "token-cache")
    public String getUser(String token) {
        if (null == token) {
            return null;
        }
        String querySession = "SELECT uid FROM tokens WHERE token=? AND eoflife>=CURRENT_TIMESTAMP";
        String queryPermanent = "SELECT uid FROM ptokens WHERE token=? AND eoflife>=CURRENT_TIMESTAMP";
        String query;
        LOG.debug("token:" + token);
        LOG.debug("permanentTokenPrefix:" + permanentTokenPrefix);
        if (token.startsWith(permanentTokenPrefix)) {
            query = queryPermanent;
        } else {
            query = querySession;
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, token);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
            return null;
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }

    @Override
    public String createSession(User user) {
        String token = java.util.UUID.randomUUID().toString();
        String query = "INSERT INTO tokens (token,uid,eoflife) VALUES (?,?,DATEADD('MINUTE', 30, CURRENT_TIMESTAMP))";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, token);
            pstmt.setString(2, user.uid);
            boolean ok = pstmt.execute();
            return token;
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
            return null;
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            return null;
        }
    }

    @Override
    public void removeSession(String token) {
        String query = "DELETE FROM tokens WHERE token=?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, token);
            boolean ok = pstmt.execute();
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

    @Override
    public void clearExpiredTokens() {
        String query = "DELETE FROM tokens WHERE eoflife<CURRENT_TIMESTAMP; "
                + "DELETE FROM ptokens WHERE eoflife<CURRENT_TIMESTAMP";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            boolean ok = pstmt.execute();
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "CALL CSVWRITE('backup/tokens.csv', 'SELECT * FROM tokens');"
                + "CALL CSVWRITE('backup/ptokens.csv', 'SELECT * FROM ptokens');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

}
