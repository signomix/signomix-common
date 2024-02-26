package com.signomix.common.db;

import com.signomix.common.Token;
import com.signomix.common.TokenType;
import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public interface AuthDaoIface {
    public void setDatasource(AgroalDataSource ds);

    public AgroalDataSource getDataSource();

    public void createStructure() throws IotDatabaseException;

    /**
     * Returns user id for given token
     * 
     * @param token
     * @return user id
     */
    public String getUserId(String token, long sessionTokenLifetime, long permanentTokenLifetime);

    /**
     * Returns issuer ID for given token
     * @param token
     * @return issuer ID
     */
    public String getIssuerId(String token, long sessionTokenLifetime, long permanentTokenLifetime);

    public Token getToken(String token, long sessionTokenLifetime, long permanentTokenLifetime);
    public Token updateToken(String token, long sessionTokenLifetime, long permanentTokenLifetime);
    /**
     * Creates new session token
     * 
     * @param user     user
     * @param lifetime token lifetime in minutes
     * @return token
     */
    //public Token createSession(User user, long lifetime);

    public Token createTokenForUser(User issuer, String userId, long lifetime, boolean permanent, TokenType tokenType, String payload);

    public void modifyToken(Token token);
    
    /**
     * Removes session token
     * 
     * @param token
     */
    public void removeSession(String token);

    public void removeToken(String token);

    public void clearExpiredTokens();

    public void backupDb() throws IotDatabaseException;
}
