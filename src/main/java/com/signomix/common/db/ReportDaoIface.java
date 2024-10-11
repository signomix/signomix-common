package com.signomix.common.db;

import java.util.List;

import io.agroal.api.AgroalDataSource;

public interface ReportDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void backupDb() throws IotDatabaseException;
    public void createStructure() throws IotDatabaseException;
    public boolean isAvailable(String className, Long userNumber, Integer organization, Integer tenant, String path) throws IotDatabaseException;
    public void saveReport(String className, Long userNumber, Integer organization, Integer tenant, String path) throws IotDatabaseException;
    public void saveReportDefinition(ReportDefinition reportDefinition) throws IotDatabaseException;
    public ReportDefinition getReportDefinition(Integer id) throws IotDatabaseException;
    //public void deleteReport(String className, Long userNumber, Integer organization, Integer tenant, String path) throws IotDatabaseException;
    public boolean deleteReportDefinition(Integer id) throws IotDatabaseException;
    public void updateReportDefinition(Integer id, ReportDefinition reportDefinition) throws IotDatabaseException;

    public List<ReportDefinition> getReportDefinitions(String userLogin) throws IotDatabaseException;
}
