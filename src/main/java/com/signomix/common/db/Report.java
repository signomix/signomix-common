package com.signomix.common.db;

import java.util.ArrayList;
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

    /**
     * Transforms the result into a CSV string
     * 
     * @param result         - the result object
     * @param datasetIndex   - index of the dataset to be transformed into CSV
     *                       format - in most cases 0
     * @param lineSeparator  - e.g. "\n" or "\r\n"
     * @param fieldSeparator - e.g. "," or ";"
     * @param includeHeader  - whether to include the header row
     * @return the CSV string
     */
    public String getAsCsv(ReportResult result, int datasetIndex, String lineSeparator, String fieldSeparator,
            boolean includeHeader) {
        StringBuilder sb = new StringBuilder();
        // check index
        if (datasetIndex >= result.datasets.size()) {
            return "";
        }
        if (datasetIndex >= result.headers.size()) {
            return "";
        }
        // write header
        if (includeHeader) {
            ArrayList<String> columns = result.headers.get(datasetIndex).columns;
            sb.append("timestamp");
            for (int i = 0; i < columns.size(); i++) {
                sb.append(fieldSeparator);
                sb.append(columns.get(i));
            }
            sb.append(lineSeparator);
        }
        // write data
        Dataset dataset = result.datasets.get(datasetIndex);
        String timestamp;
        for (int i = 0; i < dataset.data.size(); i++) {
            DatasetRow row = dataset.data.get(i);
            // format timestamp (milliseconds as long value) to ISO 8601 UTC date
            timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(row.timestamp));
            sb.append(timestamp);
            for (int j = 0; j < row.values.size(); j++) {
                sb.append(fieldSeparator);
                sb.append(row.values.get(j));
            }
            sb.append(lineSeparator);
        }
        return sb.toString();
    }

    public String getAsHtml(ReportResult result, int datasetIndex,
            boolean includeHeader) {

        String eui = "not defined";
        DataQuery query;
        try {
            query = result.queries.get("default");
            eui=query.getEui();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String output = "";

        String head = "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<style>\n" +
                "table {\n" +
                "    width: 100%;\n" +
                "    font-family: arial, sans-serif;\n" +
                "}\n" +
                "table, td, th {\n" +
                "    border: 1px solid;\n" +
                "    border-collapse: collapse;\n" +
                "}\n" +
                "th {\n" +
                "    padding: 2px;\n" +
                "    text-align: center;\n" +
                "}\n" +
                "td {\n" +
                "    padding: 2px;\n" +
                "    text-align: right;\n" +
                "}\n" +
                ".ts {\n" +
                "    text-align: left;\n" +
                "}\n" +
                "</style>\n" +
                "</head>\n";

        String tail = "</body>\n" +
                "</html>\n";

        String tableCss = " style='width: 100%; font-family: arial, sans-serif; border: 1px solid; border-collapse: collapse;'";
        String thCss = " style='padding: 2px; text-align: center; border: 1px solid; border-collapse: collapse;'";
        String tdCss = " style='padding: 2px; text-align: right; border: 1px solid; border-collapse: collapse;'";
        String tsCss = " style='padding: 2px; text-align: left; border: 1px solid; border-collapse: collapse;'";

        StringBuilder sb = new StringBuilder();
        // check index
        if (datasetIndex >= result.datasets.size()
                || datasetIndex >= result.headers.size()) {
            if (includeHeader) {
                output = head + "<h1>No data available</h1>" + tail;
            } else {
                output = "";
            }
        }

        // when html header are included, do not use inline css
        if (includeHeader) {
            tableCss = "";
            thCss = "";
            tdCss = "";
            tsCss = "";
        }

        // TODO: add report info (title, description, actual date, etc.)
        String creationDate = result.created.toString();
        creationDate = creationDate.substring(0, creationDate.indexOf(".")) + " UTC";
        String reportInfo = "<p><b>";
        if(result.title.isEmpty()) {
            reportInfo = reportInfo + "Signomix report (default)";
        }else{
            reportInfo = reportInfo + result.title;
        }
        reportInfo= reportInfo + "</b><br>"
                + "Data source ID: " + eui + "<br>"
                + "Created: "
                + creationDate + "<br>"
                + result.description + "</p>\n";
        sb.append(reportInfo);

        // write data
        // table header
        ArrayList<String> columns = result.headers.get(datasetIndex).columns;
        sb.append("<table" + tableCss + ">\n");
        sb.append("<tr>\n");
        sb.append("<th" + thCss + ">timestamp</th>\n");
        for (int i = 0; i < columns.size(); i++) {
            sb.append("<th" + thCss + ">");
            sb.append(columns.get(i));
            sb.append("</th>\n");
        }
        sb.append("</tr>\n");

        // data rows
        Dataset dataset = result.datasets.get(datasetIndex);
        String timestamp;
        for (int i = 0; i < dataset.data.size(); i++) {
            DatasetRow row = dataset.data.get(i);
            // format timestamp (milliseconds as long value) to ISO 8601 UTC date
            timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(row.timestamp));
            sb.append("<tr>\n");
            sb.append("<td" + tsCss + ">");
            sb.append(timestamp);
            sb.append("</td>\n");
            for (int j = 0; j < row.values.size(); j++) {
                sb.append("<td" + tdCss + ">");
                sb.append(row.values.get(j));
                sb.append("</td>\n");
            }
            sb.append("</tr>\n");
        }
        sb.append("</table>\n");
        if (includeHeader) {
            output = head + sb.toString() + tail;
        } else {
            output = sb.toString();
        }
        return output;
    }
}
