package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.jboss.logging.Logger;

import com.signomix.common.Organization;
import com.signomix.common.Tenant;
import com.signomix.common.User;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.OrganizationDaoIface;

import io.agroal.api.AgroalDataSource;

public class OrganizationDao implements OrganizationDaoIface {
    private static final Logger LOG = Logger.getLogger(OrganizationDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource ds) {
        this.dataSource = ds;
    }

    @Override
    public void createStructure() throws IotDatabaseException {
        // create table organizations
        StringBuilder sb = new StringBuilder();
        sb.append("create table if not exists organizations (")
                .append("id SERIAL PRIMARY KEY,")
                .append("code varchar unique,")
                .append("name varchar,")
                .append("description varchar,")
                .append("configuration jsonb default '{}'::jsonb,")
                .append("locked boolean default false,")
                .append("created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,")
                .append("updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);");
        String query = sb.toString();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        // create table tenants with root column allowing only A-Z, a-z, 0-9, - and _
        query = "CREATE TABLE IF NOT EXISTS tenants ("
                + "id SERIAL PRIMARY KEY,"
                + "organization_id INTEGER NOT NULL REFERENCES organizations(id),"
                + "name VARCHAR(255),"
                + "root VARCHAR(255) NOT NULL CHECK(root ~ '^[A-Za-z0-9_-]+$'),"
                + "configuration jsonb default '{}'::jsonb,"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        query = "insert into organizations (id,code,name,description) values (1,'','default','default organization - for accounts without organization feature');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.executeUpdate();
        } catch (SQLException e2) {
            LOG.warn("Error inserting default organization " + e2.getMessage());
            // throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
            // e2.getMessage(), e2);
        }
        // create table tenant_structure with path column type ltree
        query = "CREATE TABLE IF NOT EXISTS tenant_structure("
                + "tenant_id INTEGER REFERENCES tenants(id),"
                + "path LTREE NOT NULL,"
                + "name VARCHAR(255),"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "PRIMARY KEY (tenant_id, path));";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

        // table tenant_users
        query = "CREATE TABLE IF NOT EXISTS tenant_users("
                + "organization_id INTEGER REFERENCES organizations(id),"
                + "tenant_id INTEGER REFERENCES tenants(id),"
                + "user_id INTEGER NOT NULL," // REMOVED: REFERENCES users(user_number)
                + "path LTREE NOT NULL,"
                + "created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,"
                + "menu_definition TEXT NOT NULL DEFAULT '',"
                + "PRIMARY KEY (tenant_id, user_id));";

        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY tenants to '/var/lib/postgresql/data/export/tenants.csv' DELIMITER ';' CSV HEADER;"
                + "COPY organizations to '/var/lib/postgresql/data/export/organizations.csv' DELIMITER ';' CSV HEADER;"
                + "COPY tenant_structure to '/var/lib/postgresql/data/export/tenant_structure.csv' DELIMITER ';' CSV HEADER;"
                + "COPY tenant_users to '/var/lib/postgresql/data/export/tenant_users.csv' DELIMITER ';' CSV HEADER;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addTenant(Long organizationId, String name, String root, String configuration)
            throws IotDatabaseException {
        String query = "INSERT INTO tenants(organization_id, name, root, configuration) VALUES(?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setString(2, name);
            pst.setString(3, root);
            pst.setObject(4, configuration, Types.OTHER);
            boolean updated = pst.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void updateTenant(Integer id, Long organizationId, String name, String root, String configuration)
            throws IotDatabaseException {
        String query = "UPDATE tenants SET organization_id = ?, name = ?, root = ? , configuration=? WHERE id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setString(2, name);
            pst.setString(3, root);
            pst.setObject(4, configuration, Types.OTHER);
            pst.setInt(5, id);
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
        Tenant tenant = null;
        String query = "SELECT * FROM tenants WHERE id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setInt(1, id);
            tenant = new Tenant();
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    tenant.id = rs.getInt("id");
                    tenant.organizationId = rs.getLong("organization_id");
                    tenant.name = rs.getString("name");
                    tenant.root = rs.getString("root");
                    tenant.createdAt = rs.getTimestamp("created_at");
                    tenant.updatedAt = rs.getTimestamp("updated_at");
                    tenant.configuration = rs.getString("configuration");

                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return tenant;
    }

    @Override
    public Tenant getTenantByRoot(Long organizationId, String root) throws IotDatabaseException {
        Tenant tenant = null;
        String query = "SELECT * FROM tenants WHERE organization_id=? AND root = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setString(2, root);
            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    tenant = new Tenant();
                    tenant.id = rs.getInt("id");
                    tenant.organizationId = rs.getLong("organization_id");
                    tenant.name = rs.getString("name");
                    tenant.root = rs.getString("root");
                    tenant.createdAt = rs.getTimestamp("created_at");
                    tenant.updatedAt = rs.getTimestamp("updated_at");
                    tenant.configuration = rs.getString("configuration");
                    return tenant;
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return tenant;
    }

    @Override
    public List<Tenant> getTenants(Long organizationId, Integer limit, Integer offset) throws IotDatabaseException {
        ArrayList<Tenant> tenants = new ArrayList<Tenant>();
        String query = "SELECT * FROM tenants WHERE organization_id=? LIMIT ? OFFSET ? ";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setInt(2, limit);
            pst.setInt(3, offset);
            try (ResultSet rs = pst.executeQuery();) {
                while (rs.next()) {
                    Tenant tenant = new Tenant();
                    tenant.id = rs.getInt("id");
                    tenant.organizationId = rs.getLong("organization_id");
                    tenant.name = rs.getString("name");
                    tenant.root = rs.getString("root");
                    tenant.createdAt = rs.getTimestamp("created_at");
                    tenant.updatedAt = rs.getTimestamp("updated_at");
                    tenant.configuration = rs.getString("configuration");
                    tenants.add(tenant);
                }
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
            try (ResultSet rs = pst.executeQuery();) {
                while (rs.next()) {
                    paths.add(rs.getString("path"));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return paths;
    }

    /*
     * @Override
     * public void addTenantUser(Long organizationId, Integer tenantId, Long
     * userNumber, String path)
     * throws IotDatabaseException {
     * String query =
     * "INSERT INTO tenant_users(organization_id, tenant_id, user_id, path) VALUES(?, ?, ?, ?)"
     * ;
     * try (Connection conn = dataSource.getConnection(); PreparedStatement pst =
     * conn.prepareStatement(query);) {
     * pst.setLong(1, organizationId);
     * pst.setInt(2, tenantId);
     * pst.setLong(3, userNumber);
     * pst.setObject(4, path, java.sql.Types.OTHER);
     * boolean updated = pst.executeUpdate() > 0;
     * } catch (SQLException e) {
     * throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION,
     * e.getMessage(), e);
     * }
     * }
     */

    @Override
    public boolean canViewTenant(User user, Tenant tenant) throws IotDatabaseException {
        boolean result = false;
        String query = "SELECT * FROM tenant_users WHERE user_id = ? AND tenant_id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, user.uid);
            pst.setInt(2, tenant.id);

            try (ResultSet rs = pst.executeQuery();) {
                if (rs.next()) {
                    result = true;
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
        return result;
    }

    @Override
    public boolean canEditTenant(User user, Tenant tenant) throws IotDatabaseException {
        if (user.type == User.OWNER) {
            return true;
        }
        if (user.type == User.MANAGING_ADMIN && user.organization == tenant.organizationId) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<Organization> getOrganizations(Integer limit, Integer offset) throws IotDatabaseException {
        String query = "SELECT id,code,name,description, (select count(*) from tenants as t where t.organization_id=o.id) tenants, configuration, locked from organizations o";
        if (limit != null) {
            query += " LIMIT " + limit;
        }
        if (offset != null) {
            query += " OFFSET " + offset;
        }
        List<Organization> orgs = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            try (ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    Organization org = new Organization(
                            rs.getInt("id"),
                            rs.getString("code"),
                            rs.getString("name"),
                            rs.getString("description"),
                            rs.getString("configuration"),
                            rs.getBoolean("locked"));
                    org.setNumberOfTenants(rs.getInt("tenants"));
                    orgs.add(org);
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return orgs;
    }

    @Override
    public Organization getOrganization(long id) throws IotDatabaseException {
        Organization org = null;
        String query = "SELECT id,code,name,description, (select count(*) from tenants as t where t.organization_id=o.id) tenants, configuration, locked from organizations o where o.id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    org = new Organization(
                            rs.getInt("id"),
                            rs.getString("code"),
                            rs.getString("name"),
                            rs.getString("description"),
                            rs.getString("configuration"),
                            rs.getBoolean("locked"));
                    org.setNumberOfTenants(rs.getInt("tenants"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return org;
    }

    @Override
    public Organization getOrganization(String code) throws IotDatabaseException {
        Organization org = null;
        String query = "SELECT id,code,name,description, (select count(*) from tenants as t where t.organization_id=o.id) tenants, configuration, locked from organizations o where o.code=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, code);
            try (ResultSet rs = pstmt.executeQuery();) {
                if (rs.next()) {
                    org = new Organization(
                            rs.getInt("id"),
                            rs.getString("code"),
                            rs.getString("name"),
                            rs.getString("description"),
                            rs.getString("configuration"),
                            rs.getBoolean("locked"));
                    org.setNumberOfTenants(rs.getInt("tenants"));
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return org;
    }

    @Override
    public void addOrganization(Organization org) throws IotDatabaseException {
        String query = "INSERT INTO organizations (code,name,description,configuration) VALUES (?,?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, org.code);
            pstmt.setString(2, org.name);
            pstmt.setString(3, org.description);
            pstmt.setObject(4, org.configuration, Types.OTHER);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void updateOrganization(Organization org) throws IotDatabaseException {
        String query = "UPDATE organizations SET code=?,name=?,description=?, configuration=?, locked=? WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, org.code);
            pstmt.setString(2, org.name);
            pstmt.setString(3, org.description);
            pstmt.setObject(4, org.configuration, Types.OTHER);
            pstmt.setBoolean(5, org.locked);
            pstmt.setLong(6, org.id);
            
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void deleteOrganization(long id) throws IotDatabaseException {
        String query = "DELETE FROM organizations WHERE id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

}
