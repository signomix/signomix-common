package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.jboss.logging.Logger;

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
    public void addOrganizationStructureElement(Long organizationId, String path, String name)
            throws IotDatabaseException {
        int index=path.lastIndexOf("/");
        String parentPath=path.substring(0,index);
        Integer parentId=getStructureId(organizationId,parentPath);
        if(parentId==null){
            throw new IotDatabaseException(IotDatabaseException.NOT_FOUND, "Parent path not found: "+parentPath);
        }
        String query = "INSERT INTO org_structure (organization_id, path, name) VALUES (?, ?, ?);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setString(2, path);
            pst.setString(3, name);
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public Integer getStructureId(long organizationId, String path) throws IotDatabaseException {
        String query = "SELECT id FROM org_structure WHERE organization_id=? AND path=?;";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, organizationId);
            pst.setString(2, path);
            try (java.sql.ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                } else {
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

}
