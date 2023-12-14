package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.jboss.logging.Logger;

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
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addOrganizationStructureElement'");
    }

    @Override
    public Integer getStructureId(long organizationId, String path) throws IotDatabaseException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStructureId'");
    }

}
