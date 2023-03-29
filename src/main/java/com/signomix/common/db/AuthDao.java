package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jboss.logging.Logger;

import io.agroal.api.AgroalDataSource;

public class AuthDao implements AuthDaoIface {
    private static final Logger LOG = Logger.getLogger(AuthDao.class);

    String permanentTokenPrefix="~~";

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String getUser(String token) {
        if(null==token){
            return null;
        }
        String querySession = "SELECT uid FROM tokens WHERE token=? AND eoflife>=CURRENT_TIMESTAMP";
        String queryPermanent = "SELECT uid FROM ptokens WHERE token=? AND eoflife>=CURRENT_TIMESTAMP";
        String query;
        LOG.debug("token:"+token);
        LOG.debug("permanentTokenPrefix:"+permanentTokenPrefix);
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
        } catch(Exception ex){
            LOG.error(ex.getMessage());
            return null;
        }
    }

    @Override
    public void clearExpiredTokens(){
        String query = "DELETE FROM tokens WHERE eoflife<CURRENT_TIMESTAMP; "
        +"DELETE FROM ptokens WHERE eoflife<CURRENT_TIMESTAMP";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(query);) {
            boolean ok = pstmt.execute();
        } catch (SQLException ex) {
            LOG.warn(ex.getMessage());
        } catch(Exception ex){
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
