package com.signomix.common.db;

import io.agroal.api.AgroalDataSource;

public interface OrganizationDaoIface {
    public void setDatasource(AgroalDataSource ds);
    public void createStructure() throws IotDatabaseException;
    public Integer getStructureId(long organizationId, String path) throws IotDatabaseException;
    public void addOrganizationStructureElement(Long organizationId, String path, String name) throws IotDatabaseException;
}
