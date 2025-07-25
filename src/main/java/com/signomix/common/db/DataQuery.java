/**
 * Copyright (C) Grzegorz Skorupa 2019.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.common.db;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.signomix.common.DateTool;

/**
 *
 * @author greg
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataQuery {
    private static final Logger LOG = Logger.getLogger(DataQuery.class);

    private String source;
    private int limit;
    public int average;
    public int minimum;
    public int maximum;
    public int summary;
    private String channelName;
    private String multiplierChannelName;
    private String multiplierDeviceEui;
    private boolean timeseries;
    private String project;
    private Double newValue;
    private String eui;
    private String group;
    private Double state;
    private Timestamp fromTs;
    private Timestamp toTs;
    private Timestamp toTsExclusive;
    private boolean virtual;
    private boolean dateParamPresent;
    private long offset;
    private String className;
    private String tag;
    private String sortOrder;
    private String sortBy;
    private HashMap<String, String> parameters;
    private boolean notNull;
    private boolean skipNull;
    private Integer intervalValue;
    private String intervalName;
    private boolean firstInInterval;
    private String fromInterval; // interval from
    private String toInterval; // interval to
    private Boolean isInterval; // is interval
    private Boolean intervalTimestampAtEnd; // is interval timestamp at the end of interval
    private boolean intervalDeltas; // is interval deltas
    private boolean gapfill; // is gapfill enabled

    private String format; // format to which data should be converted (possible values: csv, html, json). JSON is default
    private Boolean forceAscendingSorting;
    private String zone;
    
    
    public void setSource(String source) {
        this.source = source;
    }

    public String getSource() {
        return source;
    }

    public Timestamp getFromTs() {
        return fromTs;
    }

    public Timestamp getToTs() {
        return toTs;
    }

    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public void setSkipNull(boolean skipNull) {
        this.skipNull = skipNull;
    }

    public boolean isSkipNull() {
        return skipNull;
    }

    public Boolean isIntervalDeltas() {
        return intervalDeltas;
    }

    public void setFormat(String format) {
        String f = format;
        if(format==null||format.isEmpty()){
            f="json";
        }
        f=f.toLowerCase();
       if(f.equals("csv")||f.equals("html")||f.equals("json")){
           this.format = f;
       }else{
              this.format = "json";
       }
    }

    public String getFormat() {
        if(format==null||format.isEmpty()){
            return "json";
        }
        return format;
    }

    /**
     * Calculates ending timestamp 1 millisecond earlier.
     * 
     * @return toTs decreased by 1 millisecond
     *
     */
    public Timestamp getToTsExclusive() {
        if (null == toTs) {
            return null;
        }
        Timestamp sooner = new Timestamp(toTs.getTime() - 1);
        sooner.setNanos(toTs.getNanos());
        return sooner;
    }

    public DataQuery() {
        limit = 0;
        average = 0;
        minimum = 0;
        maximum = 0;
        channelName = null;
        multiplierChannelName = null;
        multiplierDeviceEui = null;
        timeseries = false;
        project = null;
        newValue = null;
        eui = null;
        group = null;
        state = null;
        // fromTs = new Timestamp(0);
        // toTs = new Timestamp(System.currentTimeMillis());
        fromTs = null;
        toTs = null;
        virtual = false;
        dateParamPresent = false;
        offset = 0;
        className = null;
        toTsExclusive = null;
        tag = null;
        parameters = new HashMap<>();
        sortOrder = "DESC";
        sortBy = "timestamp";
        notNull = false;
        skipNull = false;
        intervalValue = null;
        intervalName = null;
        firstInInterval = false;
        fromInterval = null;
        toInterval = null;
        isInterval = false;
        intervalTimestampAtEnd = true;
        intervalDeltas = false;
        format = "json";
        forceAscendingSorting = false;
        zone = null; // equivalent to UTC
    }

    public String getZone() {
        // replace "_" with "/" in zone
        if(zone != null) {
            //zone = zone.replace("_", "/");
        }
        return zone;
    }

    private static String clean(String query) {
        // replace non printable characters with space
        if (query == null) {
            return "";
        }
        String q = query.replaceAll("[^\\x20-\\x7E]", " ");
        // remove multiple spaces
        q = query.replaceAll("\\s+", " ");
        // remove leading and trailing spaces
        q = q.trim();

        return q;
    }

    public static DataQuery parse(String query) throws DataQueryException {
        // TODO: in case of number format exception - log SEVERE event
        // TODO: parsing exception
        // TODO: 'to' or 'from' parameter removes 'last' (setLimit(0))
        DataQuery dq = new DataQuery();
        String q = clean(query);
        if (q.equalsIgnoreCase("last")) {
            q = "last 1";
        }
        dq.setSource(q);
        LOG.debug("data query: " + q);
        String[] params = q.split(" ");
        for (int i = 0; i < params.length;) {
            switch (params[i].toLowerCase()) {
                case "get":
                case "where":
                case "as":
                case "using":
                    i = i + 1;
                    break;
                case "limit":
                case "last": // deprecated: last is alias for limit
                    if (params[i + 1].equals("*") || params[i + 1].equals("0")) {
                        dq.setLimit(Integer.MAX_VALUE);
                    } else {
                        dq.setLimit(Integer.parseInt(params[i + 1]));
                    }
                    i = i + 2;
                    break;
                case "average":
                    dq.average = Integer.parseInt(params[i + 1]);
                    if (params.length > i + 2) {
                        try {
                            dq.setNewValue(Double.parseDouble(params[i + 2]));
                            i = i + 3;
                        } catch (NumberFormatException ex) {
                            i = i + 2;
                        }
                    } else {
                        i = i + 2;
                    }
                    break;
                case "minimum":
                    dq.minimum = Integer.parseInt(params[i + 1]);
                    if (params.length > i + 2) {
                        try {
                            dq.setNewValue(Double.parseDouble(params[i + 2]));
                            i = i + 3;
                        } catch (NumberFormatException ex) {
                            i = i + 2;
                        }
                    } else {
                        i = i + 2;
                    }
                    break;
                case "maximum":
                    dq.maximum = Integer.parseInt(params[i + 1]);
                    if (params.length > i + 2) {
                        try {
                            dq.setNewValue(Double.parseDouble(params[i + 2]));
                            i = i + 3;
                        } catch (NumberFormatException ex) {
                            i = i + 2;
                        }
                    } else {
                        i = i + 2;
                    }
                    break;
                case "sum":
                    dq.summary = Integer.parseInt(params[i + 1]);
                    if (params.length > i + 2) {
                        try {
                            dq.setNewValue(Double.parseDouble(params[i + 2]));
                            i = i + 3;
                        } catch (NumberFormatException ex) {
                            i = i + 2;
                        }
                    } else {
                        i = i + 2;
                    }
                    break;
                case "project":
                    dq.setProject(params[i + 1]);
                    i = i + 2;
                    break;
                case "class":
                case "report":
                    dq.setClassName(params[i + 1]);
                    i = i + 2;
                    break;
                case "state": {
                    try {
                        dq.setState(Double.parseDouble(params[i + 1]));
                    } catch (NumberFormatException e) {
                        // TODO:inform user about wrong query selector
                    }
                    i = i + 2;
                    break;
                }
                case "timeseries":
                case "csv.timeseries":
                    dq.setTimeseries(true);
                    i = i + 1;
                    break;
                case "virtual":
                    dq.setVirtual(true);
                    i = i + 1;
                    break;
                case "channel":
                    dq.setChannelName(params[i + 1]);
                    i = i + 2;
                    break;
                case "mpy":
                    dq.setMultiplierChannelName(params[i + 1]);
                    i= i + 2;
                    break;
                case "mpyeui":
                    dq.setMultiplierDeviceEui(params[i + 1]);
                    i = i + 2;
                    break;
                case "group":
                    dq.setGroup(params[i + 1]);
                    i = i + 2;
                    break;
                case "eui":
                    dq.setEui(params[i + 1]);
                    i = i + 2;
                    break;
                case "tag":
                    dq.setTag(params[i + 1]);
                    i = i + 2;
                    break;
                case "sort":
                    dq.sortBy = params[i + 1];
                    i = i + 2;
                    break;
                case "ascending":
                    dq.sortOrder = "ASC";
                    i = i + 1;
                    break;
                case "descending":
                    dq.sortOrder = "DESC";
                    i = i + 1;
                    break;
                case "new": {
                    try {
                        Double n = Double.parseDouble(params[i + 1]);
                        dq.setNewValue(n);
                    } catch (NumberFormatException e) {
                        // TODO:inform user about wrong query selector
                    }
                    i = i + 2;
                    break;
                }
                case "from":
                    dq.setFromTs(params[i + 1]);
                    i = i + 2;
                    dq.setLimit(0);
                    break;
                case "to":
                    dq.setToTs(params[i + 1]);
                    i = i + 2;
                    dq.setLimit(0);
                    break;
                case "sback":
                    try {
                        dq.offset = Long.parseLong(params[i + 1]);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                    i = i + 2;
                    break;
                case "notnull":
                    dq.setNotNull(true);
                    i = i + 1;
                    break;
                case "skipnull":
                    dq.setSkipNull(true);
                    i = i + 1;
                    break;
                case "interval":
                    try {
                        dq.intervalValue = Integer.parseInt(params[i + 1]);
                        dq.isInterval = true;
                    } catch (Exception e) {
                        LOG.info("error parsing query (unknown param): [" + query + "]");
                    }
                    i = i + 2;
                    break;
                case "start":
                    dq.intervalTimestampAtEnd = false;
                    i = i + 1;
                    break;
                case "end":
                    dq.intervalTimestampAtEnd = true;
                    i = i + 1;
                    break;
                case "first":
                    dq.firstInInterval = true;
                    i = i + 1;
                    break;
                case "second":
                case "minute":
                case "hour":
                case "day":
                case "week":
                case "month":
                case "quarter":
                case "year":
                    dq.intervalName = params[i];
                    i = i + 1;
                    break;
                case "deltas":
                case "delta":
                    dq.intervalDeltas = true;
                    i = i + 1;
                    break;
                case "zone":
                    dq.zone = params[i + 1];
                    i = i + 2;
                    break;
                case "gapfill":
                    dq.gapfill = true;
                    i = i + 1;
                    break;
                case "format":
                    dq.setFormat(params[i + 1]);
                    i = i + 2;
                    break;
                case "postascending":
                    dq.forceAscendingSorting = true;
                    i = i + 1;
                    break;
                default:
                    try {
                        dq.putParameter(params[i], params[i + 1]);
                    } catch (Exception e) {
                        LOG.info("error parsing query (unknown param): [" + query + "]");
                    }
                    i = i + 2;
                    break;
            }
        }

        if (dq.average > 0) {
            dq.minimum = 0;
            dq.maximum = 0;
        } else if (dq.maximum > 0) {
            dq.minimum = 0;
        }
        if (dq.limit == 0) {
            if (null != dq.fromTs || null != dq.toTs) {
                dq.limit = Integer.MAX_VALUE;
            } else {
                dq.limit = 1;
            }
        }
        if (dq.average > 0) {
            dq.setLimit(dq.average);
        } else if (dq.maximum > 0) {
            dq.setLimit(dq.maximum);
        } else if (dq.minimum > 0) {
            dq.setLimit(dq.minimum);
        }
        if (dq.isVirtual()) {
            dq.setLimit(1);
            dq.setFromTs(null);
            dq.setToTs(null);
            dq.setGroup(null);
            dq.setProject(null);
        }
        return dq;
    }

    /*
     * private static String getIntervalSymbol(String intervalStr) throws Exception{
     * String[]
     * allowed={"second","minute","hour","day","week","month","quarter","year"};
     * if(Arrays.asList(allowed).contains(intervalStr.toLowerCase())){
     * return intervalStr.toLowerCase();
     * }else{
     * throw new Exception("Invalid interval value");
     * }
     * }
     */

    public Boolean isSortingForced() {
        return forceAscendingSorting;
    }
    public Boolean isInterval() {
        return isInterval;
    }

    public Boolean isIntervalTimestampAtEnd() {
        return intervalTimestampAtEnd;
    }

    public boolean isFirstInInterval() {
        return firstInInterval;
    }

    public String getInterval() {
        return intervalValue + " " + intervalName;
    }

    public Integer getIntervalValue() {
        return intervalValue;
    }

    public String getIntervalName() {
        return intervalName;
    }

    public void setSortOrder(String order) {
        this.sortOrder = order;
    }

    public String getSortOrder() {
        return sortOrder;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void putParameter(String key, String value) {
        parameters.put(key, value);
    }

    public String getParameter(String key) {
        return parameters.get(key);
    }

    public HashMap<String, String> getParameters() {
        return parameters;
    }

    public List<String> getChannels() {
        return (null != channelName) ? (Arrays.asList(channelName.split(","))) : new ArrayList<>();
    }

    public void setMultiplierChannels(List<String> channels) {
        if (null != channels && !channels.isEmpty()) {
            multiplierChannelName = channels.get(0);
            for (int i = 1; i < channels.size(); i++) {
                multiplierChannelName += "," + channels.get(i);
            }
        }
    }

    public List<String> getMultiplierChannels() {
        return (null != multiplierChannelName) ? (Arrays.asList(multiplierChannelName.split(","))) : new ArrayList<>();
    }

    public void setChannels(List<String> channels) {
        if (null != channels && !channels.isEmpty()) {
            channelName = channels.get(0);
            for (int i = 1; i < channels.size(); i++) {
                channelName += "," + channels.get(i);
            }
        }
    }

    /**
     * @return the limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * @param limit the limit to set
     */
    public void setLimit(int limit) {
        if (dateParamPresent) {
            return;
        }
        this.limit = limit;
    }

    /*
     * public int getAverage() { return average; }
     * 
     * public void setAverage(int average) { this.average = average; }
     */
    /**
     * @return the channelName
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * @param channelName the channelName to set
     */
    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public String getMultiplierChannelName() {
        return multiplierChannelName;
    }

    public void setMultiplierChannelName(String multiplierChannelName) {
        this.multiplierChannelName = multiplierChannelName;
    }

    public String getMultiplierDeviceEui() {
        return multiplierDeviceEui;
    }

    public void setMultiplierDeviceEui(String multiplierDeviceEui) {
        this.multiplierDeviceEui = multiplierDeviceEui;
    }

    /**
     * @return the timeseries
     */
    public boolean isVirtual() {
        return virtual;
    }

    /**
     * @param timeseries the timeseries to set
     */
    public void setVirtual(boolean virtual) {
        this.virtual = virtual;
    }

    /**
     * @return the timeseries
     */
    public boolean isTimeseries() {
        return timeseries;
    }

    /**
     * @param timeseries the timeseries to set
     */
    public void setTimeseries(boolean timeseries) {
        this.timeseries = timeseries;
    }

    /**
     * @return the project
     */
    public String getProject() {
        return project;
    }

    /**
     * @param project the project to set
     */
    public void setProject(String project) {
        this.project = project;
    }

    /**
     * @return the newValue
     */
    public Double getNewValue() {
        return newValue;
    }

    /**
     * @param newValue the newValue to set
     */
    public void setNewValue(Double newValue) {
        this.newValue = newValue;
    }

    /**
     * @return the group
     */
    public String getGroup() {
        return group;
    }

    /**
     * @param group the group to set
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * @return the state
     */
    public Double getState() {
        return state;
    }

    /**
     * @param state the state to set
     */
    public void setState(Double state) {
        this.state = state;
    }

    /**
     * Parses date provided in yyyy-mm-dd_hh:mm:ss format
     * 
     * @param fromStr
     */
    public void setFromTs(String fromStr) {
        try {
            fromTs = DateTool.parseTimestamp(fromStr, null, false);
            if (null != fromTs) {
                dateParamPresent = true;
                offset = (System.currentTimeMillis() - fromTs.getTime()) / 1000;
            }
        } catch (Exception ex) {

        }
    }

    /**
     * Parses date provided in yyyy-mm-dd_hh:mm:ss format
     * 
     * @param fromStr
     */
    public void setToTs(String toStr) {
        try {
            toTs = DateTool.parseTimestamp(toStr, null, true);
            if (null != toTs) {
                dateParamPresent = true;
            }
        } catch (Exception ex) {

        }
    }

    /**
     * Get offset in seconds as calculated from fromTs parameter or provided
     * explicitly by 'sback' parameter
     * 
     * @return the offset - maximum time back from now in seconds to get data
     */
    public long getOffset() {
        return offset;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public void setEui(String eui) {
        this.eui = eui;
    }

    public String getEui() {
        return eui;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }

    public String getTagName() {
        if (tag == null) {
            return null;
        }
        return tag.split(":")[0];
    }

    public String getTagValue() {
        if (tag == null) {
            return null;
        }
        return tag.split(":")[1];
    }

    public boolean isGapfill() {
        return gapfill;
    }
}
