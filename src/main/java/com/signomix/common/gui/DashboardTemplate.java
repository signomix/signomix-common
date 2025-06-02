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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class DashboardTemplate {

    private static final Logger logger = Logger.getLogger(DashboardTemplate.class);

    private String id;
    private String title;
    private ArrayList<Object> widgets;
    private ArrayList<DashboardItem> items;
    private ArrayList<DashboardItem> itemsMobile;
    private long organizationId = 0;
    private String variables;

    public void parseVariables() {
        HashSet<String> variableSet = new HashSet<>();
        List<String> variableList;

        // Extract variables from the title
        variableList = findBracedSubstrings(title);
        for (String variable : variableList) {
            variableSet.add(variable);
        }
        // Extract variables from the widgets
        for (Object widget : widgets) {
            if (widget instanceof Widget) {
                Widget w = (Widget) widget;
                variableList = findBracedSubstrings(w.getQuery());
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings(w.getGroup());
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings(w.getDev_id());
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings(w.getTitle());
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings(w.getDescription());
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings(w.getDashboardID());
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
            } else if (widget instanceof LinkedHashMap) {
                LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) widget;
                variableList = findBracedSubstrings((String) map.get("query"));
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings((String) map.get("group"));
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings((String) map.get("dev_id"));
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings((String) map.get("title"));
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings((String) map.get("description"));
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
                variableList = findBracedSubstrings((String) map.get("dashboardID"));
                for (String variable : variableList) {
                    variableSet.add(variable);
                }
            } else {
                logger.warn("Invalid widget type in DashboardTemplate: " + widget.getClass().getName());
            }
        }
        variables = String.join(",", variableSet);
    }

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
                    // ex.printStackTrace();
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
    public ArrayList getWidgets() {
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
            try {
                w.setChartArea((Boolean) m.get("chartArea"));
            } catch (Exception e) {
                w.setChartArea(false);
            }
            try {
                w.setChartMarkers((Boolean) m.get("chartMarkers"));
            } catch (Exception e) {
                w.setChartMarkers(false);
            }
            try {
                w.setyAxisAutoScale((Boolean) m.get("yAxisAutoScale"));
            } catch (Exception e) {
                w.setyAxisAutoScale(false);
            }
            try {
                w.setAxisOptions((Boolean) m.get("axisOptions"));
            } catch (Exception e) {
                w.setAxisOptions(false);
            }
            w.setChannelTranslated((String) m.get("channelTranslated"));
            w.setType((String) m.get("type"));
            w.setChartType((String) m.get("chartType"));
            w.setTitle((String) m.get("title"));
            w.setDescription((String) m.get("description"));
            w.setQuery((String) m.get("query"));
            w.setRange((String) m.get("range"));
            w.setUnitName((String) m.get("unitName"));
            try {
                w.setWidth((Integer) m.get("width"));
            } catch (Error e) {
                // e.printStackTrace();
                w.setWidth(1);
            } catch (NullPointerException e) {
                // e.printStackTrace();
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
            w.setCommandJSON((String) m.get("commandJSON"));
            w.setCommandText((String) m.get("commandText"));
            w.setDashboardID((String) m.get("dashboardID"));
            widgets.add(w);
            /*
             * "yAxisAutoScale": false,
             * "axisOptions": false,
             * "chartArea": false,
             * "chartMarkers": false
             */
        }
    }

    /**
     * Finds all substrings enclosed in curly braces {} in the input string.
     *
     * @param input the input string to search
     * @return a list of substrings found within curly braces
     */
    public static List<String> findBracedSubstrings(String input) {
        List<String> result = new ArrayList<>();
        if (input == null || input.isEmpty()) {
            return result; // Return empty list if input is null or empty
        }
        int start = -1;
        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == '{') {
                start = i;
            } else if (input.charAt(i) == '}' && start != -1) {
                result.add(input.substring(start, i + 1));
                start = -1;
            }
        }
        return result;
    }

}
