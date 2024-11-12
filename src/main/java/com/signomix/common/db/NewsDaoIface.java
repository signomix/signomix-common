package com.signomix.common.db;

import com.signomix.common.hcms.Document;
import com.signomix.common.news.NewsDefinition;
import com.signomix.common.news.NewsEnvelope;
import com.signomix.common.news.UserNewsDto;
import io.agroal.api.AgroalDataSource;
import java.util.Map;

public interface NewsDaoIface {

    public void setDatasource(AgroalDataSource dataSource);
    public void createStructure() throws IotDatabaseException;
    public void backupDb() throws IotDatabaseException;
    public long saveNewsDefinition(NewsDefinition news) throws IotDatabaseException;  
    public void saveNewsEnvelope(NewsEnvelope newsEnvelope) throws IotDatabaseException;
    public void saveNewsDocuments(long newsId, Map<String,Document> documents) throws IotDatabaseException;
    public UserNewsDto getUserNews(String userId, String language, String typeName, Long limit, Long offset) throws IotDatabaseException;

}
