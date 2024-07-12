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
                + "id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"
                + "name VARCHAR(255),"
                + "userId VARCHAR(255),"
                + "organization BIGINT,"
                + "tenant BIGINT,"
                + "target VARCHAR(255),"
                + "type VARCHAR(255),"
                + "createdAt TIMESTAMP,"
                + "plannedAt TIMESTAMP,"
                + "publishedAt TIMESTAMP,"
                + "PRIMARY KEY (id)"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        // create index by: userId+publishedAt
        sql = "CREATE INDEX IF NOT EXISTS news_definition_userId_publishedAt ON news_definition (userId, publishedAt)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }

        sql = "CREATE TABLE IF NOT EXISTS user_news ("
                + "id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"
                + "newsId BIGINT,"
                + "userId VARCHAR(255),"
                + "language VARCHAR(10),"
                + "read TIMESTAMP,"
                + "pinned BOOLEAN,"
                + "PRIMARY KEY (id)"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        // create indexes by: newsId, userId+newsId
        sql = "CREATE INDEX IF NOT EXISTS user_news_newsId ON user_news (newsId)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        sql = "CREATE INDEX IF NOT EXISTS user_news_userId_newsId ON user_news (userId, newsId)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }

        // create table for news documents
        sql = "CREATE TABLE IF NOT EXISTS news_documents ("
                + "newsId BIGINT,"
                + "language VARCHAR(10),"
                + "title VARCHAR(255),"
                + "content TEXT,"
                + "PRIMARY KEY (id)"
                + ")";

        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        // create indexes by: newsId, newsId+language, language+title
        sql = "CREATE INDEX IF NOT EXISTS news_documents_newsId ON news_documents (newsId)";
        try (var connection = dataSource.getConnection();
                var statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
        sql = "CREATE INDEX IF NOT EXISTS news_documents_newsId_language ON news_documents (newsId, language)";
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
        Long result=null;
        String sql = "INSERT INTO news_definition (name, userId, organization, tenant, target, type, createdAt, plannedAt, publishedAt) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id";
        try{
            try (var connection = dataSource.getConnection();
                    var statement = connection.prepareStatement(sql)) {
                statement.setString(1, news.name);
                statement.setString(2, news.userId);
                statement.setLong(3, news.organization);
                statement.setLong(4, news.tenant);
                statement.setString(5, news.target);
                statement.setString(6, news.type);
                statement.setTimestamp(7, news.createdAt);
                statement.setTimestamp(8, news.plannedAt);
                statement.setTimestamp(9, news.publishedAt);
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
        String sql = "INSERT INTO user_news (newsId, userId, language, read) "
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
        String sql = "INSERT INTO news_documents (newsId, language, title, content) "
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
