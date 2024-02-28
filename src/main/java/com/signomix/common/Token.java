/*
 * Copyright 2017 Grzegorz Skorupa <g.skorupa at gmail.com>.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.signomix.common;

import java.util.Base64;

/**
 * Session token object
 *
 */
public class Token {

    public static final String PERMANENT_TOKEN_PREFIX = "~~";

    private String uid;
    private long timestamp;
    private long lifetime; // in minutes
    private String token;
    private String issuer;
    private String payload;
    private TokenType type;
    private boolean permanent;

    /**
     * Creates new session token
     * 
     * @param userID user ID
     * @param lifetime token lifetime in seconds
     * @param permanent true if token is permanent
     */
    public Token(String userID, long lifetime, boolean permanent) {
        timestamp = System.currentTimeMillis();
        setLifetime(lifetime);
        uid = userID;
        setPermanent(permanent);
        token = Base64.getUrlEncoder().encodeToString((uid + ":" + timestamp).getBytes());
        while(token.endsWith("=")){
            token=token.substring(0, token.length()-1);
        }
        if (permanent) {
            token = PERMANENT_TOKEN_PREFIX + token;
        }
    }

    public void setLifetime(long lifetime) {
        this.lifetime = lifetime;
    }

    public long getLifetime() {
        return lifetime;
    }

    /**
     * @return the uid
     */
    public String getUid() {
        return uid;
    }

    /**
     * @param uid the uid to set
     */
    public void setUid(String uid) {
        this.uid = uid;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return the token
     */
    public String getToken() {
        return token;
    }

    /**
     * @return the issuer
     */
    public String getIssuer() {
        return issuer;
    }

    /**
     * @param issuer the issuer to set
     */
    public void setIssuer(String issuer) {
        this.issuer = issuer;
    }

    /**
     * @return the payload
     */
    public String getPayload() {
        return payload;
    }

    /**
     * @param payload the payload to set
     */
    public void setPayload(String payload) {
        this.payload = payload;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public TokenType getType() {
        return type;
    }

    public void setType(TokenType type) {
        this.type = type;
    }

    public boolean isPermanent() {
        return permanent;
    }

    public void setPermanent(boolean permanent) {
        this.permanent = permanent;
    }
    
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(getToken()).append(":").append(getUid()).append(":").append(getIssuer());
        return sb.toString();
    }
}
