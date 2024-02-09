package com.signomix.common.db;

import java.util.List;

import com.signomix.common.Tenant;

import io.agroal.api.AgroalDataSource;

public interface OrganizationDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void createStructure() throws IotDatabaseException;
    public void addTenant(Integer organizationId, String name, String root) throws IotDatabaseException;
    public void updateTenant(Integer id, Integer organizationId, String name, String root) throws IotDatabaseException;
    public void deleteTenant(Integer id) throws IotDatabaseException;
    public Tenant getTenant(Integer id) throws IotDatabaseException;
    public Tenant getTenantByRoot(String root) throws IotDatabaseException;
    public List<Tenant> getTenants(Integer organizationId) throws IotDatabaseException;
    public List<String> getTenantPaths(Integer tenantId) throws IotDatabaseException;
    public void addTenantUser(Integer organizationId, Integer tenantId, Long userNumber, String path) throws IotDatabaseException;
}
