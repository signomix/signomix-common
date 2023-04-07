package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jboss.logging.Logger;

import com.signomix.common.cms.CmsException;
import com.signomix.common.cms.Document;

import io.agroal.api.AgroalDataSource;

public class CmsDao implements CmsDaoIface {
    private static final Logger LOG = Logger.getLogger(CmsDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Backup database
     * 
     * @throws IotDatabaseException
     */
    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "CALL CSVWRITE('backup/paths.csv', 'SELECT * FROM paths');"
                + "CALL CSVWRITE('backup/tags.csv', 'SELECT * FROM tags');"
                + "CALL CSVWRITE('backup/wip_pl.csv', 'SELECT * FROM wip_pl');"
                + "CALL CSVWRITE('backup/published_pl.csv', 'SELECT * FROM published_pl');"
                + "CALL CSVWRITE('backup/wip_en.csv', 'SELECT * FROM wip_en');"
                + "CALL CSVWRITE('backup/published_en.csv', 'SELECT * FROM published_en');"
                + "CALL CSVWRITE('backup/wip_it.csv', 'SELECT * FROM wip_it');"
                + "CALL CSVWRITE('backup/published_it.csv', 'SELECT * FROM published_it');"
                + "CALL CSVWRITE('backup/wip_fr.csv', 'SELECT * FROM wip_fr');"
                + "CALL CSVWRITE('backup/published_fr.csv', 'SELECT * FROM published_fr');";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    /**
     * Creates database structure
     * 
     * @throws IotDatabaseException
     */
    @Override
    public void createStructure() throws IotDatabaseException {
        String query = "CREATE TABLE IF NOT EXISTS urls ("
                + "source VARCHAR,"
                + "target VARCHAR PRIMARY KEY); "
                + "CREATE INDEX IF NOT EXISTS idxurls ON urls(source);";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

    /**
     * Creates a new document
     * 
     * @param rs ResultSet with document data
     * @throws IotDatabaseException
     */
    Document buildDocument(ResultSet rs) throws CmsException, SQLException {
        Document doc = new Document();
        doc.setUid(rs.getString(1));
        doc.setAuthor(rs.getString(2));
        doc.setType(rs.getString(3));
        doc.setTitle(rs.getString(4));
        doc.setSummary(rs.getString(5));
        doc.setContent(rs.getString(6));
        doc.setTags(rs.getString(7));
        doc.setLanguage(rs.getString(8));
        doc.setMimeType(rs.getString(9));
        doc.setStatus(rs.getString(10));
        doc.setCreatedBy(rs.getString(11));
        doc.setSize(rs.getLong(12));
        doc.setCommentable(rs.getBoolean(13));
        doc.setCreated(rs.getTimestamp(14).toInstant().toString());
        doc.setModified(rs.getTimestamp(15).toInstant().toString());
        try {
            doc.setPublished(rs.getTimestamp(16).toInstant().toString());
        } catch (NullPointerException e) {
        }
        doc.setExtra(rs.getString(17));
        return doc;
    }

    /**
     * Removes all paths that are not used by any document
     * 
     * @throws IotDatabaseException
     */
    @Override
    public void doCleanup() throws IotDatabaseException {
        String query = "DELETE"
                + " FROM paths"
                + " WHERE paths.path NOT IN ("
                + " SELECT path FROM published_pl"
                + " UNION"
                + " SELECT path FROM published_en"
                + " UNION"
                + " SELECT path FROM published_fr"
                + " UNION"
                + " SELECT path FROM published_it"
                + " UNION"
                + " SELECT path FROM wip_pl"
                + " UNION"
                + " SELECT path FROM wip_en"
                + " UNION"
                + " SELECT path FROM wip_fr"
                + " UNION"
                + " SELECT path FROM wip_it"
                + ");";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage());
        }
    }

}
