/**
* Copyright (C) Grzegorz Skorupa 2018.
* Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
*/
package com.signomix.common.iot;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class DeviceGroup {

    private String EUI; 
    private String name;
    private String userID;
    private String team;
    private String administrators;
    private transient HashMap channels;
    private String channelsAsString;
    private String description;
    private boolean open;
    private long organization;

    public long getOrganization() {
        return organization;
    }

    public void setOrganization(long organization) {
        this.organization = organization;
    }

    //TODO: change uid to uidHex and add validation (is it hex value)
    public DeviceGroup() {
        EUI = ""; 
        userID = "";
        team = "";
        administrators = "";
        description = "";
        open=true;
        channelsAsString="";
    }

    public boolean userIsTeamMember(String userID) {
        String[] t = team.split(",");
        for (int i = 0; i < t.length; i++) {
            if (userID.equals(t[i])) {
                return true;
            }
        }
        String[] adm = administrators.split(",");
        for (int i = 0; i < t.length; i++) {
            if (userID.equals(t[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the EUI
     */
    public String getEUI() {
        return EUI.toUpperCase();
    }

    /**
     * @param EUI the EUI to set
     */
    public void setEUI(String EUI) {
        this.EUI = EUI.toUpperCase();
    }

    /**
     * @return the userID
     */
    public String getUserID() {
        return userID;
    }

    /**
     * @param userID the userID to set
     */
    public void setUserID(String userID) {
        this.userID = userID;
    }

    /**
     * @return the team
     */
    public String getTeam() {
        return team;
    }

    /**
     * @param team the team to set
     */
    public void setTeam(String team) {
        this.team = team==null?"":team;
        if (!this.team.startsWith(",")) {
            this.team = "," + this.team;
        }
        if (!this.team.endsWith(",")) {
            this.team = this.team + ",";
        }
        setOpen(this.team.indexOf(",public,")>=0);
    }

    public String getAdministrators() {
        return administrators;
    }

    /**
     * @param team the team to set
     */
    public void setAdministrators(String administrators) {
        this.administrators = administrators!=null?administrators:"";
        if (!this.administrators.startsWith(",")) {
            this.administrators = "," + this.administrators;
        }
        if (!this.administrators.endsWith(",")) {
            this.administrators = this.administrators + ",";
        }
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }


    public HashMap getChannels() {
        return channels;
    }
    
/*     public String getChannelsAsString() {
        if(null==channels){
            return "";
        }
        StringBuilder sb = new StringBuilder();
        channels.keySet().forEach(key -> {
            sb.append(key).append(",");
        });
        String result = sb.toString();
        if (!result.isEmpty()) {
            result = result.substring(0, result.length() - 1);
        }
        return result.toLowerCase();
    } */

    public String getChannelsAsString() {
        return channelsAsString;
    }
    
    public void setChannels(LinkedHashMap channels) {
        this.channels = channels;
    }

    public void setChannels(String channels) {
        this.channels = parseChannels(channels);
    }

    public void setChannelsAsString(String channels) {
        this.channelsAsString = channels;
        setChannels(channels);
    }
    
    private LinkedHashMap parseChannels(String channelsDeclaration) {
        LinkedHashMap result = new LinkedHashMap();
        if (channelsDeclaration != null) {
            String[] channelArray = channelsDeclaration.toLowerCase().split(",");
            for (String tmp : channelArray) {
                if(tmp.isEmpty()){
                    continue;
                }
                Channel c = new Channel();
                c.setName(tmp.trim().toLowerCase());
                result.put(c.getName(), c);
            }
        }
        return result;
    }

    /**
     * @return the open
     */
    public boolean isOpen() {
        return open;
    }

    /**
     * @param open the open to set
     */
    public void setOpen(boolean open) {
        this.open = open;
    }

}
