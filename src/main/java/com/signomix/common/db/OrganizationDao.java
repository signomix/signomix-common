package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.Organization;
import com.signomix.common.Tenant;
import com.signomix.common.User;

import io.agroal.api.AgroalDataSource;

public class OrganizationDao implements OrganizationDaoIface {
    private static final Logger LOG = Logger.getLogger(UserDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource ds) {
        this.dataSource = ds;
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        String query = "CREATE TABLE org_structure("
                + "id SERIAL PRIMARY KEY,"
                + "organization_id INTEGER NOT NULL REFERENCES organizations(id),"
                + "path VARCHAR(255) NOT NULL,"
                + "update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + "name VARCHAR(255) );";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

        query = "CREATE INDEX organization_id_path_index ON org_structure ('organization_id', 'path');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e2) {
            LOG.warn("Error inserting default organization");
        }

        query = "CREATE TABLE org_users("
                + "user_number INTEGER REFERENCES users(user_number),"
                + "structure_id INTEGER REFERENCES org_structure(id),"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "PRIMARY KEY (user_number, structure_id));";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void addTenant(Long organizationId, String name, String root, String menuDefinition) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addTenant'");
    }

    @Override
    public void updateTenant(Integer id, Long organizationId, String name, String root, String menuDefinition) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateTenant'");
    }

    @Override
    public void deleteTenant(Integer id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteTenant'");
    }

    @Override
    public Tenant getTenant(Integer id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTenant'");
    }

    @Override
    public Tenant getTenantByRoot(Long organizationId, String root) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTenantByRoot'");
    }

    @Override
    public List<Tenant> getTenants(Long organizationId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTenants'");
    }

    @Override
    public List<String> getTenantPaths(Integer tenantId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTenantPaths'");
    }

    @Override
    public void addTenantUser(Long organizationId, Integer tenantId, Long userNumber, String path)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addTenantUser'");
    }

    @Override
    public boolean canViewTenant(User user, Tenant tenant) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'canViewwTenant'");
    }

    @Override
    public boolean canEditTenant(User user, Tenant tenant) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'canEditTenant'");
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'backupDb'");
    }

    @Override
    public List<Organization> getOrganizations(Integer limit, Integer offset) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrganizations'");
    }

    @Override
    public Organization getOrganization(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrganization'");
    }

    @Override
    public Organization getOrganization(String code) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrganization'");
    }

    @Override
    public void deleteOrganization(long id) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'deleteOrganization'");
    }

    @Override
    public void addOrganization(Organization organization) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addOrganization'");
    }

    @Override
    public void updateOrganization(Organization organization) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateOrganization'");
    }

}
