package com.signomix.common.db;

import java.util.List;

import com.signomix.common.Organization;
import com.signomix.common.Tenant;
import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public interface OrganizationDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void createStructure() throws IotDatabaseException;
    public void backupDb() throws IotDatabaseException;
    public void addTenant(Long organizationId, String name, String root, String menuDefinition) throws IotDatabaseException;
    public void updateTenant(Integer id, Long organizationId, String name, String root, String menuDefinition) throws IotDatabaseException;
    public void deleteTenant(Integer id) throws IotDatabaseException;
    public Tenant getTenant(Integer id) throws IotDatabaseException;
    public Tenant getTenantByRoot(Long organizationId, String root) throws IotDatabaseException;
    public List<Tenant> getTenants(Long organizationId, Integer limit, Integer offset) throws IotDatabaseException;
    public List<String> getTenantPaths(Integer tenantId) throws IotDatabaseException;
    //public void addTenantUser(Long organizationId, Integer tenantId, Long userNumber, String path) throws IotDatabaseException;
    public boolean canViewTenant(User user, Tenant tenant) throws IotDatabaseException;
    public boolean canEditTenant(User user, Tenant tenant) throws IotDatabaseException;

    public List<Organization> getOrganizations(Integer limit, Integer offset) throws IotDatabaseException;
    public Organization getOrganization(long id) throws IotDatabaseException;
    public Organization getOrganization(String code) throws IotDatabaseException;
    public void deleteOrganization(long id) throws IotDatabaseException;
    public void addOrganization(Organization organization) throws IotDatabaseException;
    public void updateOrganization(Organization organization) throws IotDatabaseException;
}
