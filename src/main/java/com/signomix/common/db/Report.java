package com.signomix.common.db;

import java.util.HashMap;
import org.jboss.logging.Logger;

public class Report {
    private static final Logger logger = Logger.getLogger(Report.class);

    public HashMap<String, Object> options = new HashMap<>();

    public void setOptions(HashMap<String, Object> options) {
        this.options = options;
        // print out options
        /*
         * logger.info("Set options");
         * Iterator it=this.options.keySet().iterator();
         * while(it.hasNext()){
         * String key=(String)it.next();
         * logger.info("Option "+key+":"+this.options.get(key));
         * }
         */

    }

    public boolean isAuthorized() {
        return true;
    }

    public ReportResult sortResult(ReportResult result, String reportName) {
        Dataset dataset = null;
        int datasetIndex = -1;
        for (int i = 0; i < result.datasets.size(); i++) {
            if (result.datasets.get(i).name.equals(reportName)) {
                dataset = result.datasets.get(i);
                datasetIndex = i;
                break;
            }
        }
        if (dataset == null) {
            logger.error("Dataset not found: " + reportName);
            return result;
        }
        DataQuery query = result.queries.get(reportName);
        if (query == null) {
            logger.warn("Query not found: " + reportName);
            return result;
        }

        // sort dataset rows by column specified in query
        logger.debug("sorting by timestamp " + query.getSortOrder());
        dataset.data.sort((DatasetRow o1, DatasetRow o2) -> {
            long val1 = o1.timestamp;
            long val2 = o2.timestamp;
            if (query.getSortOrder().equals("DESC")) {
                return Long.compare(val2, val1);
            } else {
                return Long.compare(val1, val2);
            }
        });
        result.datasets.set(datasetIndex, dataset);
        return result;
    }
}
