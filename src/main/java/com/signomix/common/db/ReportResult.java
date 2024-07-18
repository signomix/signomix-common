package com.signomix.common.db;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReportResult {
    public List<Dataset> datasets;
    public List<DatasetHeader> headers;
    public String title = null;
    public String description = null;
    public Long id = null;
    public Timestamp created = null;
    // public DataQuery query=null;
    public HashMap<String, DataQuery> queries;
    public String content = null;
    public String contentType = null;
    public Integer status = 200;
    public String errorMessage=null;

    public static Logger logger = Logger.getLogger(ReportResult.class);

    /**
     * Parse a serialized ReportResult object and return a ReportResult object
     * 
     * @param serialized
     * @return
     */
    public static ReportResult parse(String serialized) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(serialized, ReportResult.class);
        } catch (JsonProcessingException ex) {
            logger.error("Error parsing ReportResult object: "+ex.getMessage());
            return null;
        }
    }

    public ReportResult() {
        datasets = new ArrayList<Dataset>();
        headers = new ArrayList<DatasetHeader>();
        queries = new HashMap<String, DataQuery>();
    }

    public ReportResult(DataQuery query) {
        this();
        this.setQuery("default", query);
    }

    public ReportResult error(String message) {
        this.title = "";
        this.description = "";
        this.status = 400;
        this.errorMessage = message;
        return this;
    }

    public ReportResult error(int code, String message) {
        this.title = "";
        this.description = "";
        this.status = code;
        this.errorMessage = message;
        return this;
    }

    /**
     * Get header names for a dataset of a given name
     * 
     * @param datasetName
     * @return ArrayList<String>
     */
    public DatasetHeader getHeaders(String datasetName) {
        return headers.stream().filter(d -> d.name.equals(datasetName)).findFirst().orElse(null);
    }

    /**
     * Get data for a dataset of a given name
     * 
     * @param datasetName
     * @return ArrayList<ArrayList<Double>>
     */
    public Dataset getData(String datasetName) {
        return datasets.stream().filter(d -> d.name.equals(datasetName)).findFirst().orElse(null);
    }

    public void addDatasetHeader(DatasetHeader header) {
        headers.add(header);
    }

    public void addDataset(Dataset dataset) {
        datasets.add(dataset);
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.created = timestamp;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public DataQuery getQuery(String name) {
        return queries.get(name);
    }

    public void setQuery(String name, DataQuery query) {
        queries.put(name, query);
    }

}