package com.signomix.common.db;

import java.util.HashMap;

import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public interface ReportIface {

        /**
         * Get report result as ReportResult object.
         * This is the method to be used primarily by desktop widgets.
         * 
         * @param olapDs
         * @param oltpDs
         * @param logsDs
         * @param query
         * @param organization
         * @param tenant
         * @param path
         * @param user
         * @return
         */
        public ReportResult getReportResult(
                        AgroalDataSource olapDs,
                        AgroalDataSource oltpDs,
                        AgroalDataSource logsDs,
                        DataQuery query,
                        Integer organization,
                        Integer tenant,
                        String path,
                        User user);

        /**
         * Get report result as ReportResult object.
         * This is the method to be used primarily by mobile widgets.
         * 
         * @param olapDs
         * @param oltpDs
         * @param logsDs
         * @param query
         * @param user
         * @return
         */
        public ReportResult getReportResult(
                        AgroalDataSource olapDs,
                        AgroalDataSource oltpDs,
                        AgroalDataSource logsDs,
                        DataQuery query,
                        User user);

        /**
         * Get report result as HTML string.
         * 
         * @param olapDs
         * @param oltpDs
         * @param logsDs
         * @param query
         * @param organization
         * @param tenant
         * @param path
         * @param user
         * @return
         */
        public String getReportHtml(
                        AgroalDataSource olapDs,
                        AgroalDataSource oltpDs,
                        AgroalDataSource logsDs,
                        DataQuery query,
                        Integer organization,
                        Integer tenant,
                        String path,
                        User user,
                        Boolean withHeader);

        /**
         * Get report result as HTML string.
         * 
         * @param olapDs
         * @param oltpDs
         * @param logsDs
         * @param query
         * @param user
         * @return
         */
        public String getReportHtml(
                        AgroalDataSource olapDs,
                        AgroalDataSource oltpDs,
                        AgroalDataSource logsDs,
                        DataQuery query,
                        User user,
                        Boolean withHeader);

        /**
         * Get report result as CSV string.
         * 
         * @param olapDs
         * @param oltpDs
         * @param logsDs
         * @param query
         * @param organization
         * @param tenant
         * @param path
         * @param user
         * @return
         */
        public String getReportCsv(
                        AgroalDataSource olapDs,
                        AgroalDataSource oltpDs,
                        AgroalDataSource logsDs,
                        DataQuery query,
                        Integer organization,
                        Integer tenant,
                        String path,
                        User user);

        /**
         * Get report result as CSV string.
         * 
         * @param olapDs
         * @param oltpDs
         * @param logsDs
         * @param query
         * @param user
         * @return
         */
        public String getReportCsv(
                        AgroalDataSource olapDs,
                        AgroalDataSource oltpDs,
                        AgroalDataSource logsDs,
                        DataQuery query,
                        User user);


        /**
         * Get report result as CSV string.
         * 
         * @param olapDs
         * @param oltpDs
         * @param logsDs
         * @param query
         * @param organization
         * @param tenant
         * @param path
         * @param user
         * @return
         */
        public String getReportFormat(
                        AgroalDataSource olapDs,
                        AgroalDataSource oltpDs,
                        AgroalDataSource logsDs,
                        DataQuery query,
                        Integer organization,
                        Integer tenant,
                        String path,
                        User user,
                        String format);

        /**
         * Get report result as CSV string.
         * 
         * @param olapDs
         * @param oltpDs
         * @param logsDs
         * @param query
         * @param user
         * @return
         */
        public String getReportFormat(
                        AgroalDataSource olapDs,
                        AgroalDataSource oltpDs,
                        AgroalDataSource logsDs,
                        DataQuery query,
                        User user,
                        String format);

        
        /**
         * Set report options.
         * @param options
         */
        public void setOptions(HashMap<String, Object> options);

}
