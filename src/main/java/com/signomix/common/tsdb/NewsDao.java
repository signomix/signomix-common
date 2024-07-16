package com.signomix.common.tsdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.NewsDaoIface;
import com.signomix.common.hcms.Document;
import com.signomix.common.news.NewsDefinition;
import com.signomix.common.news.NewsEnvelope;

import io.agroal.api.AgroalDataSource;

public class NewsDao implements NewsDaoIface {

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Creates the table for news definitions.
     * Tables are:
     * - news_definition for storing object of type NewsDefinition
     * - user_news for storing object of type NewsEnvelope
     */
    @Override
    public void createStructure() throws IotDatabaseException {

        String sql = "CREATE TABLE IF NOT EXISTS news_definition ("
                + "id BIGSERIAL PRIMARY KEY,"
                + "name VARCHAR(255),"
                + "user_id VARCHAR(255),"
                + "organization BIGINT,"
                + "tenant BIGINT,"
                + "target VARCHAR(255),"
                + "type VARCHAR(255),"
                + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + "planned_at TIMESTAMP,"
                + "published_at TIMESTAMP"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        // create index by: user_id+publishedAt
        sql = "CREATE INDEX IF NOT EXISTS news_definition_user_id_publishedAt ON news_definition (user_id, published_at)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }

        sql = "CREATE TABLE IF NOT EXISTS user_news ("
                + "id BIGSERIAL PRIMARY KEY,"
                + "news_id BIGINT,"
                + "user_id VARCHAR(255),"
                + "language VARCHAR(10),"
                + "read TIMESTAMP,"
                + "pinned BOOLEAN"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        // create indexes by: news_id, user_id+news_id
        sql = "CREATE INDEX IF NOT EXISTS user_news_news_id ON user_news (news_id)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        sql = "CREATE INDEX IF NOT EXISTS user_news_user_id_news_id ON user_news (user_id, news_id)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }

        // create table for news documents
        sql = "CREATE TABLE IF NOT EXISTS news_documents ("
                + "news_id BIGINT,"
                + "language VARCHAR(10),"
                + "title VARCHAR(255),"
                + "content TEXT"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        // create indexes by: news_id, news_id+language, language+title
        sql = "CREATE INDEX IF NOT EXISTS news_documents_news_id ON news_documents (news_id)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        sql = "CREATE INDEX IF NOT EXISTS news_documents_news_id_language ON news_documents (news_id, language)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        sql = "CREATE INDEX IF NOT EXISTS news_documents_language_title ON news_documents (language, title)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }

    }

    @Override
    public long saveNewsDefinition(NewsDefinition news) throws IotDatabaseException {
        Long result = null;
        if (news.name == null) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, "Missing required field: name");
        }
        if (news.type == null) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, "Missing required field: type");
        }
        String sql = "INSERT INTO news_definition (name, user_id, organization, tenant, target, type, planned_at, published_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id";
        try {
            try (var connection = dataSource.getConnection();
                    var statement = connection.prepareStatement(sql)) {

                statement.setString(1, news.name);
                if (news.userId != null) {
                    statement.setString(2, news.userId);
                } else {
                    statement.setNull(2, java.sql.Types.VARCHAR);
                }
                if (news.organization == null) {
                    statement.setNull(3, java.sql.Types.BIGINT);
                } else {
                    statement.setLong(3, news.organization);
                }
                if (news.tenant == null) {
                    statement.setNull(4, java.sql.Types.BIGINT);
                } else {
                    statement.setLong(4, news.tenant);
                }
                if (news.target == null) {
                    statement.setNull(5, java.sql.Types.VARCHAR);
                } else {
                    statement.setString(5, news.target);
                }
                statement.setString(6, news.type);
                if (news.plannedAt == null) {
                    statement.setNull(7, java.sql.Types.TIMESTAMP);
                } else {
                    statement.setTimestamp(7, news.plannedAt);
                }
                if (news.publishedAt == null) {
                    statement.setNull(8, java.sql.Types.TIMESTAMP);
                } else {
                    statement.setTimestamp(9, news.publishedAt);
                }
                var rs = statement.executeQuery();
                if (rs.next()) {
                    result = rs.getLong(1);
                }
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        return result;
    }

    @Override
    public void saveNewsEnvelope(NewsEnvelope newsEnvelope) throws IotDatabaseException {
        String sql = "INSERT INTO user_news (news_id, user_id, language, read) "
                + "VALUES (?, ?, ?, ?)";

        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(sql)) {
            statement.setLong(1, newsEnvelope.newsId);
            statement.setString(2, newsEnvelope.userId);
            statement.setString(3, newsEnvelope.language);
            statement.setTimestamp(4, newsEnvelope.read);
            statement.executeUpdate();
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void saveNewsDocuments(long newsId, Map<String, Document> documents) throws IotDatabaseException {
        String sql = "INSERT INTO news_documents (news_id, language, title, content) "
                + "VALUES (?, ?, ?, ?)";
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(sql)) {
            for (String key : documents.keySet()) {
                Document doc = documents.get(key);
                String title = doc.metadata.get("title");
                if (title == null) {
                    title = doc.name;
                }
                statement.setLong(1, newsId);
                statement.setString(2, key);
                statement.setString(3, title);
                statement.setString(4, doc.content);
                statement.executeUpdate();
            }
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public void backupDb() throws IotDatabaseException {
        String query = "COPY news_definition to '/var/lib/postgresql/data/export/news_definition.csv' DELIMITER ';' CSV HEADER;"
                + "COPY user_news to '/var/lib/postgresql/data/export/user_news.csv' DELIMITER ';' CSV HEADER;"
                + "COPY news_documents to '/var/lib/postgresql/data/export/news_documents.csv' DELIMITER ';' CSV HEADER;";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {
            statement.execute();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

}
