package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.Tenant;

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
    public void addTenant(Integer organizationId, String name, String root) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addTenant'");
    }

    @Override
    public void updateTenant(Integer id, Integer organizationId, String name, String root) throws IotDatabaseException {
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
    public Tenant getTenantByRoot(String root) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTenantByRoot'");
    }

    @Override
    public List<Tenant> getTenants(Integer organizationId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTenants'");
    }

    @Override
    public List<String> getTenantPaths(Integer tenantId) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTenantPaths'");
    }

    @Override
    public void addTenantUser(Integer organizationId, Integer tenantId, Long userNumber, String path)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addTenantUser'");
    }

}
