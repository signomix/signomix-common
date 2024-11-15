package com.signomix.common.tsdb;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.NewsDaoIface;
import com.signomix.common.hcms.Document;
import com.signomix.common.news.NewsDefinition;
import com.signomix.common.news.NewsEnvelope;
import com.signomix.common.news.UserNewsDto;
import io.agroal.api.AgroalDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

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

    @Override
    public UserNewsDto getUserNews(String userId, String language, String typeName, Long limit, Long offset)
            throws IotDatabaseException {
        String sql = "SELECT n.id, n.news_id, n.user_id, n.language, n.read, n.pinned, d.title, nd.type, nd.created_at, COUNT(*) OVER() AS total "
                + "FROM user_news n "
                + "JOIN news_documents d ON n.news_id = d.news_id AND n.language = d.language "
                + "JOIN news_definition nd ON n.news_id = nd.id "
                + "WHERE n.user_id = ? AND n.language = ? ";
        if (typeName != null) {
            sql += "AND LOWER(nd.type() = LOWER(?) ";
        }
        sql += "ORDER BY n.pinned ASC, nd.id DESC";
        if (limit != null) {
            sql += " LIMIT ?";
        }
        if (offset != null) {
            sql += " OFFSET ?";
        }

        UserNewsDto result = new UserNewsDto();
        NewsEnvelope envelope;
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(sql)) {
            statement.setString(1, userId);
            statement.setString(2, language);
            int idx = 3;
            if (typeName != null) {
                statement.setString(2, typeName);
                idx++;
            }
            if (limit != null) {
                statement.setLong(idx, limit);
                idx++;
            }
            if (offset != null) {
                statement.setLong(idx, offset);
            }
            var rs = statement.executeQuery();
            while (rs.next()) {
                envelope = new NewsEnvelope();
                envelope.id = rs.getLong("id");
                envelope.newsId = rs.getLong("news_id");
                envelope.userId = rs.getString("user_id");
                envelope.language = rs.getString("language");
                envelope.created = rs.getTimestamp("created_at");
                envelope.read = rs.getTimestamp("read");
                if (rs.wasNull()) {
                    envelope.read = null;
                }
                envelope.pinned = rs.getBoolean("pinned");
                if (rs.wasNull()) {
                    envelope.pinned = false;
                }
                envelope.title = rs.getString("title");
                result.news.add(envelope);
                result.size = rs.getInt("total");
            }
            return result;
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Document getNewsDocument(long newsId, String language) throws IotDatabaseException {
        String sql = "SELECT title, content, language FROM news_documents WHERE news_id = ? AND language = ?";
        Document result = new Document();
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(sql)) {
            statement.setLong(1, newsId);
            statement.setString(2, language);
            var rs = statement.executeQuery();
            if (rs.next()) {
                result.metadata.put("title", rs.getString("title"));
                result.metadata.put("language", rs.getString("language"));
                result.content = rs.getString("content");
            }
            return result;
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage());
        }
    }

}
