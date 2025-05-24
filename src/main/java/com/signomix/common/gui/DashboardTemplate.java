/**
 * Copyright (C) Grzegorz Skorupa 2022.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.common.gui;

import com.cedarsoftware.util.io.JsonReader;
import com.cedarsoftware.util.io.JsonWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class DashboardTemplate {

    private String id;
    private String name;
    private String title;
    private ArrayList<Widget> widgets;
    private ArrayList<DashboardItem> items;
    private ArrayList<DashboardItem> itemsMobile;
    private long organizationId = 0;
    private String variables;

    public String getVariables() {
        return variables;
    }

    public void setVariables(String variables) {
        this.variables = variables;
    }

    public DashboardTemplate() {
        id = null;
        widgets = new ArrayList<>();
        items = new ArrayList<>();
    }

    public DashboardTemplate(String newID) {
        id = newID;
        widgets = new ArrayList<>();
        items = new ArrayList<>();
    }

    public void addWidget(Widget widget) {
        widgets.add(widget);
    }

    public void setWidget(int index, Object widget) {
        widgets.add(index, (Widget) widget);
    }

    public ArrayList getItems() {
        return items;
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
                    //ex.printStackTrace();
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
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

    public long getOrganizationId() {
        return organizationId;
    }

    public void setOrganizationId(long id) {
        this.organizationId = id;
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
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
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
    public ArrayList<Widget> getWidgets() {
        return widgets;
    }

    /**
     * @param widgets the widgets to set
     */
    public void setWidgets(ArrayList widgets) {
        this.widgets = widgets;
    }

    public String getWidgetsAsJson() {
        return JsonWriter.objectToJson(widgets);
    }

    public void setWidgetsFromJson(String jsonString) {
        ArrayList<Map> wl = (ArrayList) JsonReader.jsonToJava(jsonString);
        for (int i = 0; i < wl.size(); i++) {
            Map m = wl.get(i);
            Widget w = new Widget();
            w.setName((String) m.get("name"));
            w.setDev_id((String) m.get("dev_id"));
            w.setChannel((String) m.get("channel"));
            w.setChartOption((String) m.get("chartOption"));
            w.setChannelTranslated((String) m.get("channelTranslated"));
            w.setType((String) m.get("type"));
            w.setTitle((String) m.get("title"));
            w.setDescription((String) m.get("description"));
            w.setQuery((String) m.get("query"));
            w.setRange((String) m.get("range"));
            w.setUnitName((String) m.get("unitName"));
            try {
                w.setWidth((Integer) m.get("width"));
            } catch (Error e) {
                e.printStackTrace();
                w.setWidth(1);
            } catch (NullPointerException e) {
                e.printStackTrace();
                w.setWidth(1);
            }
            w.setModified(false);
            w.setGroup((String) m.get("group"));
            w.setApp_id((String) m.get("app_id"));
            w.setFormat((String) m.get("format"));
            w.setCommandType((String) m.get("commandType"));
            w.setRole((String) m.get("role"));
            w.setUnit((String) m.get("unit"));
            w.setRounding((String) m.get("rounding"));
            w.setIcon((String) m.get("icon"));
            w.setConfig((String) m.get("config"));
            widgets.add(w);
        }
    }

}
