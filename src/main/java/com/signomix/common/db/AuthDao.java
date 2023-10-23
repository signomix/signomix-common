package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Singleton;

import org.jboss.logging.Logger;

import com.signomix.common.Token;
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
    public AgroalDataSource getDataSource() {
        return dataSource;
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        String query = "CREATE TABLE IF NOT EXISTS tokens (token varchar primary key, uid varchar, issuer varchar, payload varchar, tstamp timestamp default CURRENT_TIMESTAMP, eoflife timestamp);"
                + "CREATE TABLE IF NOT EXISTS ptokens (token varchar primary key, uid varchar, issuer varchar, payload varchar, tstamp timestamp default CURRENT_TIMESTAMP, eoflife timestamp);";
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
    public String getUserId(String token, long sessionTokenLifetime, long permanentTokenLifetime) {
        // TODO: update eoflife
        // TODO: RETURNING can be used for PostgreSQL
        try {
            String userUid=null;
            LOG.info("getUser: " + token);
            if (null == token) {
                return null;
            }
            String querySession = "SELECT uid FROM tokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";
            String queryPermanent = "SELECT uid FROM ptokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";

            String updateSession = "UPDATE tokens SET eoflife=DATEADD('MINUTE', ?, CURRENT_TIMESTAMP) WHERE token=?";
            String updatePermanent = "UPDATE ptokens SET eoflife=DATEADD('MINUTE', ?, CURRENT_TIMESTAMP) WHERE token=?";
            String query, updateQuery;
            long lifetime = 0;
            LOG.debug("token:" + token);
            LOG.debug("permanentTokenPrefix:" + permanentTokenPrefix);
            if (token.startsWith(permanentTokenPrefix)) {
                query = queryPermanent;
                updateQuery = updatePermanent;
                lifetime = permanentTokenLifetime;
            } else {
                query = querySession;
                updateQuery = updateSession;
                lifetime = sessionTokenLifetime;
            }
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, token);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    LOG.info("getUserId: token found: " + token);
                    userUid=rs.getString("uid");
                } else {
                    LOG.warn("getUserId: token not found: " + token);
                }
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
            if (userUid!=null) {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(updateQuery);) {
                    pstmt.setLong(1, lifetime);
                    pstmt.setString(2, token);
                    int count = pstmt.executeUpdate();
                    LOG.info("getUserId: updated " + count + " rows");
                } catch (SQLException ex) {
                    LOG.warn(ex.getMessage());
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                }
            }
            return userUid;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return null;
        }
    }

/*     @Override
    public Token createSession(User user, long lifetime) {
        String token = java.util.UUID.randomUUID().toString();
        Token t = new Token(user.uid, lifetime, false);
        String query = "INSERT INTO tokens (token,uid,eoflife) VALUES (?,?,DATEADD('MINUTE', ?, CURRENT_TIMESTAMP))";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, token);
            pstmt.setString(2, user.uid);
            pstmt.setLong(3, lifetime);
            int count = pstmt.executeUpdate();
            LOG.info("createSession: inserted " + count + " rows");
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
            t=null;
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            t=null;
        }
        return t;
    } */

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

    @Override
    public Token createTokenForUser(User issuer, String userId, long lifetime, boolean permanent) {
        try {
            LOG.info("createTokenForUser: " + userId + " " + lifetime + " " + permanent);
            //String token = java.util.UUID.randomUUID().toString();
            Token t = new Token(userId, lifetime, permanent);
            t.setIssuer(issuer.uid);
            String query;
            if (permanent) {
                query = "INSERT INTO ptokens (token,uid,tstamp,eoflife,issuer) VALUES (?,?,CURRENT_TIMESTAMP,DATEADD('MINUTE', ?, CURRENT_TIMESTAMP),?)";
            } else {
                query = "INSERT INTO tokens (token,uid,tstamp,eoflife,issuer) VALUES (?,?,CURRENT_TIMESTAMP,DATEADD('MINUTE', ?, CURRENT_TIMESTAMP),?)";
            }
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, t.getToken());
                pstmt.setString(2, t.getUid());
                pstmt.setLong(3, t.getLifetime());
                pstmt.setString(4, t.getIssuer());
                int count = pstmt.executeUpdate();
                LOG.info("createTokenForUser: inserted " + count + " rows");
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
                return null;
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
                return null;
            }
            return t;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String getIssuerId(String token,long sessionTokenLifetime, long permanentTokenLifetime) {
        // TODO: update eoflife
        // TODO: RETURNING can be used for PostgreSQL
        try {
            String userUid=null;
            LOG.info("getIssuer: " + token);
            if (null == token) {
                return null;
            }
            String querySession = "SELECT uid FROM tokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";
            String queryPermanent = "SELECT uid FROM ptokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";

            String updateSession = "UPDATE tokens SET eoflife=DATEADD('MINUTE', ?, CURRENT_TIMESTAMP) WHERE token=?";
            String updatePermanent = "UPDATE ptokens SET eoflife=DATEADD('MINUTE', ?, CURRENT_TIMESTAMP) WHERE token=?";
            String query, updateQuery;
            long lifetime = 0;
            LOG.debug("token:" + token);
            LOG.debug("permanentTokenPrefix:" + permanentTokenPrefix);
            if (token.startsWith(permanentTokenPrefix)) {
                query = queryPermanent;
                updateQuery = updatePermanent;
                lifetime = permanentTokenLifetime;
            } else {
                query = querySession;
                updateQuery = updateSession;
                lifetime = sessionTokenLifetime;
            }
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, token);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    LOG.info("getUserId: token found: " + token);
                    userUid=rs.getString("uid");
                } else {
                    LOG.warn("getUserId: token not found: " + token);
                }
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
            if (userUid!=null) {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(updateQuery);) {
                    pstmt.setLong(1, lifetime);
                    pstmt.setString(2, token);
                    int count = pstmt.executeUpdate();
                    LOG.info("getUserId: updated " + count + " rows");
                } catch (SQLException ex) {
                    LOG.warn(ex.getMessage());
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                }
            }
            return userUid;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return null;
        }
    }

    public Token getToken(String tokenID, long sessionTokenLifetime, long permanentTokenLifetime) {
        // TODO: update eoflife
        // TODO: RETURNING can be used for PostgreSQL
        Token token = null;
        try {
            LOG.info("getIssuer: " + tokenID);
            if (null == tokenID) {
                return null;
            }
            String querySession = "SELECT * FROM tokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";
            String queryPermanent = "SELECT * FROM ptokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";

            String updateSession = "UPDATE tokens SET eoflife=(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute') WHERE token=?";
            String updatePermanent = "UPDATE ptokens SET eoflife=(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute') WHERE token=?";
            String query, updateQuery;
            long lifetime = 0;
            LOG.debug("token:" + tokenID);
            LOG.debug("permanentTokenPrefix:" + permanentTokenPrefix);
            if (tokenID.startsWith(permanentTokenPrefix)) {
                query = queryPermanent;
                updateQuery = updatePermanent;
                lifetime = permanentTokenLifetime;
            } else {
                query = querySession;
                updateQuery = updateSession;
                lifetime = sessionTokenLifetime;
            }
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, tokenID);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    LOG.info("getUserId: token found: " + tokenID);
                    token = new Token(rs.getString("uid"), rs.getLong("eoflife"),
                            tokenID.startsWith(permanentTokenPrefix));
                    token.setIssuer(rs.getString("issuer"));
                    token.setPayload(rs.getString("payload"));
                    token.setTimestamp(rs.getTimestamp("tstamp").getTime());
                    token.setToken(rs.getString("token"));
                } else {
                    LOG.warn("getUserId: token not found: " + tokenID);
                }
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
            if (token != null) {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt = conn.prepareStatement(updateQuery);) {
                    pstmt.setLong(1, lifetime);
                    pstmt.setString(2, tokenID);
                    int count = pstmt.executeUpdate();
                    LOG.info("getUserId: updated " + count + " rows");
                } catch (SQLException ex) {
                    LOG.warn(ex.getMessage());
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                }
            }
            return token;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return null;
        }
    }

}
