/**
 * Copyright (C) Grzegorz Skorupa 2019.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.common.db;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.signomix.common.DateTool;
import org.jboss.logging.Logger;

/**
 *
 * @author greg
 */
public class DataQuery {
    private static final Logger LOG = Logger.getLogger(DataQuery.class);

    private int limit;
    public int average;
    public int minimum;
    public int maximum;
    public int summary;
    private String channelName;
    private boolean timeseries;
    private String project;
    private Double newValue;
    private String group;
    private Double state;
    private Timestamp fromTs;
    private Timestamp toTs;
    private boolean virtual;
    private boolean dateParamPresent;
    private long offset;

    public Timestamp getFromTs() {
        return fromTs;
    }

    public Timestamp getToTs() {
        return toTs;
    }

    /**
     * Calculates ending timestamp 1 millisecond earlier.
     * 
     * @return toTs decreased by 1 millisecond
     *
     */
    public Timestamp getToTsExclusive() {
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
        timeseries = false;
        project = null;
        newValue = null;
        group = null;
        state = null;
        // fromTs = new Timestamp(0);
        // toTs = new Timestamp(System.currentTimeMillis());
        fromTs = null;
        toTs = null;
        virtual = false;
        dateParamPresent = false;
        offset = 0;
    }

    public static DataQuery parse(String query) throws DataQueryException {
        // TODO: in case of number format exception - log SEVERE event
        // TODO: parsing exception
        // TODO: 'to' or 'from' parameter removes 'last' (setLimit(0))
        LOG.debug(query);
        DataQuery dq = new DataQuery();
        String q = query.trim();
        if (q.equalsIgnoreCase("last")) {
            q = "last 1";
        }
        String[] params = q.split(" ");
        for (int i = 0; i < params.length;) {
            switch (params[i].toLowerCase()) {
                case "get":
                case "where":
                case "as":
                    i = i + 1;
                    break;
                case "last":
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
                case "group":
                    dq.setGroup(params[i + 1]);
                    i = i + 2;
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
                default:
                    throw new DataQueryException(DataQueryException.PARSING_EXCEPTION,
                            "unrecognized word " + params[i]);
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

    public List<String> getChannels() {
        return (null != channelName) ? (Arrays.asList(channelName.split(","))) : new ArrayList<>();
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
                offset = System.currentTimeMillis() - fromTs.getTime();
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
     * Get offset in milliseconds as calculated from fromTs parameter.
     * @return the offset - milliseconds from fromTs to current time
     */
    public long getOffset() {
        return offset;
    }

}
