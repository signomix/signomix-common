/**
 * Copyright (C) Grzegorz Skorupa 2018.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.common.gui;

import java.util.ArrayList;
import java.util.HashSet;

import com.cedarsoftware.util.io.JsonReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class Dashboard {

    private String id;
    private String name;
    private String userID;
    private String title;
    private boolean shared;
    private String team;
    private ArrayList<Widget> widgets;
    private ArrayList<DashboardItem> items;
    private String sharedToken;
    private String administrators;
    private int version = 0;
    private long organizationId = 0;
    private boolean favourite = false;

    public Dashboard() {
        id = null;
        shared = false;
        widgets = new ArrayList<>();
        items = new ArrayList<>();
    }

    public Dashboard(String newID) {
        id = newID;
        shared = false;
        widgets = new ArrayList<>();
        items = new ArrayList<>();
    }

    public void applyTemplate(DashboardTemplate template, String deviceEui) {
        ArrayList<Widget> widgets = template.getWidgets();
        widgets.forEach(widget -> {
            widget.setDev_id(deviceEui);
        });
        setWidgets(widgets);
    }

    public void setVersion(int v) {
        version = v;
    }

    public int getVersion() {
        return version;
    }

    public void addWidget(Widget widget) {
        widgets.add(widget);
    }

    public void setWidget(int index, Widget widget) {
        widgets.add(index, widget);
    }

    /**
     * @return the team
     */
    public String[] teamToArray() {
        return getTeam().split(",");
    }

    public boolean isTeamMember(String userId) {
        return team.indexOf("," + userId + ",") > -1;
        // return Arrays.stream(teamToArray()).anyMatch(userId::equals);
    }

    /**
     * @return the id
     */
    public String getId() {
        return id.toUpperCase();
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id.toUpperCase();
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
     * @return the shared
     */
    public boolean isShared() {
        return shared;
    }

    /**
     * @param shared the shared to set
     */
    public void setShared(boolean shared) {
        this.shared = shared;
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
        String tmp = team;
        if (!tmp.startsWith(",")) {
            tmp = "," + tmp;
        }
        if (!tmp.endsWith(",")) {
            tmp = tmp + ",";
        }
        this.team = tmp;
    }

    public String getAdministrators() {
        return administrators;
    }

    /**
     * @param administrators the administrators to set
     */
    public void setAdministrators(String administrators) {
        String tmp = administrators != null ? administrators.trim() : "";
        if (!tmp.startsWith(",")) {
            tmp = "," + tmp;
        }
        if (!tmp.endsWith(",")) {
            tmp = tmp + ",";
        }
        this.administrators = tmp;
    }

    /**
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * @return the widgets
     */
    public ArrayList getWidgets() {
        return widgets;
    }

    /**
     * @param widgets the widgets to set
     */
    public void setWidgets(ArrayList widgets) {
        this.widgets = widgets;
    }

    public ArrayList getItems() {
        return items;
    }

    /**
     * @param widgets the widgets to set
     */
    public void setItems(ArrayList items) {
        this.items = items;
    }

    public void addItem(DashboardItem item) {
        items.add(item);
    }

    /**
     * @return the sharedToken
     */
    public String getSharedToken() {
        return sharedToken;
    }

    /**
     * @param sharedToken the sharedToken to set
     */
    public void setSharedToken(String sharedToken) {
        this.sharedToken = sharedToken;
    }

    /**
     * @return the organizationId
     */
    public long getOrganizationId() {
        return organizationId;
    }

    /**
     * @param organizationId the organizationId to set
     */
    public void setOrganizationId(long organizationId) {
        this.organizationId = organizationId;
    }

    /**
     * @return the favourite
     */
    public boolean isFavourite() {
        return favourite;
    }

    /**
     * @param favourite the favourite to set
     */
    public void setFavourite(boolean favourite) {
        this.favourite = favourite;
    }

    public String getWidgetsAsJson() {
        // if (null == items || items.isEmpty()) {
        // return JsonWriter.objectToJson(widgets);
        // } else {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(widgets);
        } catch (JsonProcessingException ex) {
            return "";
        }
        // }
    }

    public void setWidgetsFromJson(String jsonString) {
        try {
            if (jsonString.indexOf("@type") > 0) {
                widgets = (ArrayList) JsonReader.jsonToJava(jsonString);
            } else {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    widgets = objectMapper.readValue(jsonString, ArrayList.class);
                } catch (JsonProcessingException ex) {
                    //
                }
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
    }

    public String getItemsAsJson() {
        // return JsonWriter.objectToJson(items);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(items);
        } catch (JsonProcessingException ex) {
            ex.printStackTrace();
            return "";
        }
    }

    public void setItemsFromJson(String jsonString) {
        try {
            if (jsonString.indexOf("@type") > 0) {
                items = (ArrayList) JsonReader.jsonToJava(jsonString);
            } else {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    items = objectMapper.readValue(jsonString, ArrayList.class);
                } catch (JsonProcessingException ex) {
                    // ex.printStackTrace();
                }
            }
        } catch (Exception e) {
            // e.printStackTrace();
        }
    }

    public Dashboard normalize() {
        String tmp = getTeam();
        if (!tmp.startsWith(",")) {
            tmp = "," + tmp;
        }
        if (!tmp.endsWith(",")) {
            tmp = tmp + ",";
        }
        setTeam(tmp);
        /*
         * for(int i =0; i<widgets.size(); i++){
         * ((Widget)widgets.get(i)).normalize();
         * 
         * }
         */
        return this;
    }

    public HashSet<String> getDeviceEuis() {
        HashSet<String> euis = new HashSet<>();
        for (int i = 0; i < widgets.size(); i++) {
            Widget w = (Widget) widgets.get(i);
            if (w.getDev_id() != null && !w.getDev_id().isEmpty()) {
                euis.add(w.getDev_id());
            }
        }
        return euis;
    }

    public HashSet<String> getGroupEuis() {
        HashSet<String> euis = new HashSet<>();
        for (int i = 0; i < widgets.size(); i++) {
            Widget w = (Widget) widgets.get(i);
            if (w.getGroup() != null && !w.getGroup().isEmpty()) {
                euis.add(w.getGroup());
            }
        }
        return euis;
    }

}
