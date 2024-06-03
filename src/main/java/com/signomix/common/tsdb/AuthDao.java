package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Singleton;

import org.jboss.logging.Logger;

import com.signomix.common.Token;
import com.signomix.common.TokenType;
import com.signomix.common.User;
import com.signomix.common.db.AuthDaoIface;
import com.signomix.common.db.IotDatabaseException;

import io.agroal.api.AgroalDataSource;
import io.quarkus.cache.CacheResult;

@Singleton
public class AuthDao implements AuthDaoIface {
    private static final Logger LOG = Logger.getLogger(AuthDao.class);

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
        String query = "CREATE TYPE token_type AS ENUM ('SESSION', 'API', 'PERMANENT', 'RESET_PASSWORD', 'EMAIL_VERIFICATION', 'DASHBOARD', 'CONFIRM');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            LOG.warn(e.getMessage());
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }

        query = "CREATE TABLE IF NOT EXISTS tokens ("
                + "token varchar primary key,"
                + "uid varchar,"
                + "issuer varchar,"
                + "payload varchar,"
                + "tstamp timestamp default CURRENT_TIMESTAMP,"
                + "eoflife timestamp,"
                + "type token_type NOT NULL DEFAULT 'SESSION'::token_type);"
                + "CREATE TABLE IF NOT EXISTS ptokens ("
                + "token varchar primary key,"
                + "uid varchar,"
                + "issuer varchar,"
                + "payload varchar,"
                + "tstamp timestamp default CURRENT_TIMESTAMP,"
                + "eoflife timestamp,"
                + "type token_type NOT NULL DEFAULT 'PERMANENT'::token_type);"
                + "CREATE INDEX IF NOT EXISTS tokens_token_eoflife_index ON tokens (token, eoflife);"
                + "CREATE INDEX IF NOT EXISTS ptokens_token_eoflife_index ON ptokens (token, eoflife);";
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
            String userUid = null;
            LOG.info("getUser: " + token);
            if (null == token) {
                return null;
            }
            String querySession = "SELECT uid FROM tokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";
            String queryPermanent = "SELECT uid FROM ptokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";

            String updateSession = "UPDATE tokens SET eoflife=(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute') WHERE token=?";
            String updatePermanent = "UPDATE ptokens SET eoflife=(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute') WHERE token=?";
            String query, updateQuery;
            long lifetime = 0;
            LOG.debug("token:" + token);
            if (token.startsWith(Token.PERMANENT_TOKEN_PREFIX) || token.startsWith(Token.API_TOKEN_PREFIX)) {
                query = queryPermanent;
                updateQuery = null;
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
                    userUid = rs.getString("uid");
                } else {
                    LOG.warn("getUserId: token not found: " + token);
                }
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
            if (userUid != null && updateQuery != null) {
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
    public void removeToken(String token) {
        String query;
        if (token.startsWith(Token.PERMANENT_TOKEN_PREFIX) || token.startsWith(Token.API_TOKEN_PREFIX)) {
            query = "DELETE FROM ptokens WHERE token=?";
        } else {
            query = "DELETE FROM tokens WHERE token=?";
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, token);
            pstmt.setString(2, token);
            boolean ok = pstmt.execute();
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

    @Override
    public void removeDashboardToken(String dashboardId) {
        String query = "DELETE FROM ptokens WHERE payload=? AND type='DASHBOARD'";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, dashboardId);
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
        String query = "COPY tokens to '/var/lib/postgresql/data/export/tokens.csv' DELIMITER ';' CSV HEADER;"
                + "COPY ptokens to '/var/lib/postgresql/data/export/ptokens.csv' DELIMITER ';' CSV HEADER;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Token createTokenForUser(User issuer, String userId, long lifetime, boolean permanent, TokenType tokenType,
            String payload) {
        try {
            LOG.info("createTokenForUser: " + userId + " " + lifetime + " " + permanent);
            // String token = java.util.UUID.randomUUID().toString();
            Token t = new Token(userId, lifetime, permanent);
            t.setIssuer(issuer.uid);
            String query;
            if (permanent) {
                query = "INSERT INTO ptokens (token,uid,tstamp,eoflife,issuer,type,payload) VALUES (?,?,CURRENT_TIMESTAMP,(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute'),?,?,?)";
            } else {
                query = "INSERT INTO tokens (token,uid,tstamp,eoflife,issuer,type,payload) VALUES (?,?,CURRENT_TIMESTAMP,(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute'),?,?,?)";
            }
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, t.getToken());
                pstmt.setString(2, t.getUid());
                pstmt.setLong(3, t.getLifetime());
                pstmt.setString(4, t.getIssuer());
                pstmt.setObject(5, tokenType.name(), java.sql.Types.OTHER);
                pstmt.setString(6, payload);
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
    public void saveToken(Token token) {
        String query;
        if (token.isPermanent()) {
            query = "INSERT INTO ptokens (token,uid,tstamp,eoflife,issuer,type,payload) VALUES (?,?,CURRENT_TIMESTAMP,(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute'),?,?,?)";
        } else {
            query = "INSERT INTO tokens (token,uid,tstamp,eoflife,issuer,type,payload) VALUES (?,?,CURRENT_TIMESTAMP,(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute'),?,?,?)";
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, token.getToken());
            pstmt.setString(2, token.getUid());
            pstmt.setLong(3, token.getLifetime());
            pstmt.setString(4, token.getIssuer());
            pstmt.setObject(5, token.getType().name(), java.sql.Types.OTHER);
            pstmt.setString(6, token.getPayload());
            int count = pstmt.executeUpdate();
            LOG.info("saveToken: inserted " + count + " rows");
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

    @Override
    public void modifyToken(Token token) {
        LOG.info("modifyToken: " + token.getToken() + " " + token.getLifetime() + " " + token.getPayload() + " "
                + token.getIssuer() + " " + token.getType() + " " + token.getUid() + " " + token.isPermanent());
        String query;
        if (token.isPermanent()) {
            query = "UPDATE ptokens SET eoflife=(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute'),"
                    + " payload=?, issuer=?, type=?, uid=?, tstamp=CURRENT_TIMESTAMP WHERE token=?";
        } else {
            query = "UPDATE tokens SET eoflife=(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute'),"
                    + " payload=?, issuer=?, type=?, uid=?, tstamp=CURRENT_TIMESTAMP WHERE token=?";
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, token.getLifetime());
            pstmt.setString(2, token.getPayload());
            pstmt.setString(3, token.getIssuer());
            pstmt.setObject(4, token.getType().name(), java.sql.Types.OTHER);
            pstmt.setString(5, token.getUid());
            pstmt.setString(6, token.getToken());
            int count = pstmt.executeUpdate();
            LOG.info("modifyToken " + token.getToken() + " " + (token.isPermanent() ? "permanent" : "session")
                    + ": updated " + count + " rows");
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

    @Override
    public String getIssuerId(String token, long sessionTokenLifetime, long permanentTokenLifetime) {
        // TODO: update eoflife
        // TODO: RETURNING can be used for PostgreSQL
        try {
            String userUid = null;
            LOG.info("getIssuer: " + token);
            if (null == token) {
                return null;
            }
            String querySession = "SELECT uid FROM tokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";
            String queryPermanent = "SELECT uid FROM ptokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";

            /*
             * String updateSession =
             * "UPDATE tokens SET eoflife=(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute') WHERE token=?"
             * ;
             * String updatePermanent =
             * "UPDATE ptokens SET eoflife=(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute') WHERE token=?"
             * ;
             */ String query, updateQuery;
            long lifetime = 0;
            LOG.debug("token:" + token);
            if (token.startsWith(Token.PERMANENT_TOKEN_PREFIX) || token.startsWith(Token.API_TOKEN_PREFIX)) {
                query = queryPermanent;
                // updateQuery = updatePermanent;
                lifetime = permanentTokenLifetime;
            } else {
                query = querySession;
                // updateQuery = updateSession;
                lifetime = sessionTokenLifetime;
            }
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, token);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    LOG.info("getUserId: token found: " + token);
                    userUid = rs.getString("uid");
                } else {
                    LOG.warn("getUserId: token not found: " + token);
                }
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
            /*
             * if (userUid != null) {
             * try (Connection conn = dataSource.getConnection();
             * PreparedStatement pstmt = conn.prepareStatement(updateQuery);) {
             * pstmt.setLong(1, lifetime);
             * pstmt.setString(2, token);
             * int count = pstmt.executeUpdate();
             * LOG.info("getUserId: updated " + count + " rows");
             * } catch (SQLException ex) {
             * LOG.warn(ex.getMessage());
             * } catch (Exception ex) {
             * LOG.error(ex.getMessage());
             * }
             * }
             */
            return userUid;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return null;
        }
    }

    public Token getToken(String tokenID, long sessionTokenLifetime, long permanentTokenLifetime) {
        Token token = null;
        try {
            LOG.info("getToken: " + tokenID);
            if (null == tokenID) {
                return null;
            }
            String querySession = "SELECT * FROM tokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";
            String queryPermanent = "SELECT * FROM ptokens WHERE token=? AND"
                    + " eoflife>=CURRENT_TIMESTAMP";

            String query;
            long lifetime = 0;
            LOG.debug("token:" + tokenID);
            if (tokenID.startsWith(Token.PERMANENT_TOKEN_PREFIX) || tokenID.startsWith(Token.API_TOKEN_PREFIX)) {
                query = queryPermanent;
                lifetime = permanentTokenLifetime;
            } else {
                query = querySession;
                lifetime = sessionTokenLifetime;
            }
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, tokenID);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    LOG.info("getUserId: token found: " + tokenID);
                    token = new Token(rs.getString("uid"), lifetime,
                            tokenID.startsWith(Token.PERMANENT_TOKEN_PREFIX));
                    token.setIssuer(rs.getString("issuer"));
                    token.setPayload(rs.getString("payload"));
                    token.setTimestamp(rs.getTimestamp("tstamp").getTime());
                    token.setToken(rs.getString("token"));
                    token.setType(TokenType.valueOf(rs.getString("type")));
                    token.setPayload(rs.getString("payload"));
                } else {
                    LOG.warn("getUserId: token not found: " + tokenID);
                }
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
                ex.printStackTrace();
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
                ex.printStackTrace();
            }
            return token;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return null;
        }
    }

    public Token updateToken(String tokenID, long sessionTokenLifetime, long permanentTokenLifetime) {
        // TODO: update eoflife
        // TODO: RETURNING can be used for PostgreSQL

        if(tokenID==null || tokenID.startsWith(Token.API_TOKEN_PREFIX)){
            return null;
        }

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
            if (tokenID.startsWith(Token.PERMANENT_TOKEN_PREFIX)) {
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
                    token = new Token(rs.getString("uid"), lifetime,
                            tokenID.startsWith(Token.PERMANENT_TOKEN_PREFIX));
                    token.setIssuer(rs.getString("issuer"));
                    token.setPayload(rs.getString("payload"));
                    token.setTimestamp(rs.getTimestamp("tstamp").getTime());
                    token.setToken(rs.getString("token"));
                } else {
                    LOG.warn("getUserId: token not found: " + tokenID);
                }
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
                ex.printStackTrace();
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
                ex.printStackTrace();
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
                    ex.printStackTrace();
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                    ex.printStackTrace();
                }
            }
            return token;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return null;
        }
    }

    @Override
    public Token findTokenById(String tokenId) {
        String query;
        Token token = null;
        // TODO: check eolife
        if (tokenId.startsWith(Token.PERMANENT_TOKEN_PREFIX) || tokenId.startsWith(Token.API_TOKEN_PREFIX)) {
            query = "SELECT * FROM ptokens WHERE token=? AND eoflife>=CURRENT_TIMESTAMP";
        } else {
            query = "SELECT * FROM tokens WHERE token=? AND eoflife>=CURRENT_TIMESTAMP";
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, tokenId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                token = new Token(rs.getString("uid"), 0, tokenId.startsWith(Token.PERMANENT_TOKEN_PREFIX));
                token.setIssuer(rs.getString("issuer"));
                token.setPayload(rs.getString("payload"));
                token.setTimestamp(rs.getTimestamp("tstamp").getTime());
                token.setToken(rs.getString("token"));
                token.setType(TokenType.valueOf(rs.getString("type")));
                token.setPayload(rs.getString("payload"));
            } else {
                LOG.warn("findTokenById: token not found: " + tokenId);
            }
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
            ex.printStackTrace();
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            ex.printStackTrace();
        }
        return token;
    }


    @Override
    public Token createApiToken(User issuer, long lifetimeMinutes){
        try {
            LOG.info("createApiToken: " + issuer.uid + " " + lifetimeMinutes);
            Token t = new Token(issuer.uid, lifetimeMinutes, TokenType.API);
            t.setIssuer(issuer.uid);
            String query = "INSERT INTO ptokens (token,uid,tstamp,eoflife,issuer,type,payload) VALUES (?,?,CURRENT_TIMESTAMP,(CURRENT_TIMESTAMP + ? * INTERVAL '1 minute'),?,?,?)";
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt = conn.prepareStatement(query);) {
                pstmt.setString(1, t.getToken());
                pstmt.setString(2, t.getUid());
                pstmt.setLong(3, t.getLifetime());
                pstmt.setString(4, t.getIssuer());
                pstmt.setObject(5, t.getType().name(), java.sql.Types.OTHER);
                pstmt.setString(6, "");
                int count = pstmt.executeUpdate();
                return t;
            } catch (SQLException ex) {
                LOG.warn(ex.getMessage());
                return null;
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Token getApiToken(User user) {
        String query = "SELECT * FROM ptokens WHERE uid=? AND type='API' AND eoflife>=CURRENT_TIMESTAMP";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, user.uid);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Token token = new Token(rs.getString("uid"), 0, true);
                token.setIssuer(rs.getString("issuer"));
                token.setPayload(rs.getString("payload"));
                token.setTimestamp(rs.getTimestamp("tstamp").getTime());
                token.setToken(rs.getString("token"));
                token.setType(TokenType.valueOf(rs.getString("type")));
                token.setPayload(rs.getString("payload"));
                token.setLifetime(rs.getTimestamp("eoflife").getTime() - rs.getTimestamp("tstamp").getTime());
                return token;
            } else {
                LOG.warn("getApiToken: token not found: " + user.uid);
            }
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
            ex.printStackTrace();
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            ex.printStackTrace();
        }
        return null;
    }

}
