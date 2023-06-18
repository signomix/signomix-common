package com.signomix.common.db;

import java.util.List;

import com.signomix.common.gui.Dashboard;
import com.signomix.common.gui.DashboardTemplate;

import io.agroal.api.AgroalDataSource;

public interface DashboardIface {
    public void setDatasource(AgroalDataSource ds);
    public void backupDb() throws IotDatabaseException;
    public void createStructure() throws IotDatabaseException;
    public void addDashboard(Dashboard dashboard) throws IotDatabaseException;
    public void removeDashboard(String dashboardId) throws IotDatabaseException;
    public Dashboard getDashboard(String dashboardId) throws IotDatabaseException;
    public void updateDashboard(Dashboard dashboard) throws IotDatabaseException;
    public DashboardTemplate getDashboardTemplate(String dashboardTemplateId) throws IotDatabaseException;
    public void saveAsTemplate(Dashboard dashboard) throws IotDatabaseException;
    public void removeTemplate(String dashboardTemplateId) throws IotDatabaseException;
    public List<Dashboard> getUserDashboards(String userId, Integer limit, Integer offset) throws IotDatabaseException;
    public List<Dashboard> getDashboards(Integer limit, Integer offset)throws IotDatabaseException;
}