package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.Tenant;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.OrganizationDaoIface;

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
        // create table tenants with root column allowing only A-Z, a-z, 0-9, - and _
        String query = "CREATE TABLE IF NOT EXISTS tenants("
        + "id SERIAL PRIMARY KEY,"
        + "organization_id INTEGET NOT NULL REFERENCES organizations(id),"
        + "name VARCHAR(255),"
        + "root_VARCHAR(255) NOT NULL CHECK (root ~ '^[A-Za-z0-9_-]+$'),"
        + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP),"
        + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

        // create table tenant_structure with path column type ltree
        query = "CREATE TABLE IF NOT EXISTS tenant_structure("
                + "tenant_id INTEGER REFERENCES tenants(id),"
                + "path LTREE NOT NULL,"
                + "name VARCHAR(255),"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"
                + "PRIMARY KEY (tenant_id, path);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

        // table tenant_users
        query = "CREATE TABLE IF NOT EXISTS tenant_users("
                + "organization_id INTEGER REFERENCES organizations(id),"
                + "tenant_id INTEGER REFERENCES tenants(id),"
                + "user_id INTEGER REFERENCES users(id),"
                + "path LTREE NOT NULL,"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"
                + "PRIMARY KEY (tenant_id, user_id);";

        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

    }

    @Override
    public void addTenant(Integer organizationId, String name, String root) throws IotDatabaseException {
        String query = "INSERT INTO tenants(organization_id, name, root) VALUES(?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, organizationId);
            pst.setString(2, name);
            pst.setString(3, root);
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void updateTenant(Integer id, Integer organizationId, String name, String root) throws IotDatabaseException {
        String query = "UPDATE tenants SET organization_id = ?, name = ?, root = ? WHERE id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, organizationId);
            pst.setString(2, name);
            pst.setString(3, root);
            pst.setInt(4, id);
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void deleteTenant(Integer id) throws IotDatabaseException {
        String query = "DELETE FROM tenants WHERE id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, id);
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public Tenant getTenant(Integer id) throws IotDatabaseException {
        String query = "SELECT * FROM tenants WHERE id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, id);
            Tenant tenant = new Tenant();
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                tenant.id = rs.getInt("id");
                tenant.organizationId = rs.getInt("organization_id");
                tenant.name = rs.getString("name");
                tenant.root = rs.getString("root");
                tenant.createdAt = rs.getTimestamp("created_at");
                tenant.updatedAt = rs.getTimestamp("updated_at");
                return tenant;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return null;
    }

    @Override
    public Tenant getTenantByRoot(String root) throws IotDatabaseException {
        String query = "SELECT * FROM tenants WHERE root = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, root);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                Tenant tenant = new Tenant();
                tenant.id = rs.getInt("id");
                tenant.organizationId = rs.getInt("organization_id");
                tenant.name = rs.getString("name");
                tenant.root = rs.getString("root");
                tenant.createdAt = rs.getTimestamp("created_at");
                tenant.updatedAt = rs.getTimestamp("updated_at");
                return tenant;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return null;
    }

    @Override
    public List<Tenant> getTenants(Integer organizationId) throws IotDatabaseException {
        ArrayList<Tenant> tenants = new ArrayList<Tenant>();
        String query = "SELECT * FROM tenants WHERE organization_id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, organizationId);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                Tenant tenant = new Tenant();
                tenant.id = rs.getInt("id");
                tenant.organizationId = rs.getInt("organization_id");
                tenant.name = rs.getString("name");
                tenant.root = rs.getString("root");
                tenant.createdAt = rs.getTimestamp("created_at");
                tenant.updatedAt = rs.getTimestamp("updated_at");
                tenants.add(tenant);
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return tenants;
    }

    @Override
    public List<String> getTenantPaths(Integer tenantId) throws IotDatabaseException {
        ArrayList<String> paths = new ArrayList<String>();
        String query = "SELECT path FROM tenant_structure WHERE tenant_id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, tenantId);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                paths.add(rs.getString("path"));
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return paths;
    }

    @Override
    public void addTenantUser(Integer organizationId, Integer tenantId, Long userNumber, String path)
            throws IotDatabaseException {
        String query = "INSERT INTO tenant_users(organization_id, tenant_id, user_id, path) VALUES(?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, organizationId);
            pst.setInt(2, tenantId);
            pst.setLong(3, userNumber);
            pst.setString(4, path);
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }


}
