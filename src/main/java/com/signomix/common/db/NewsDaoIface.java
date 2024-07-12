package com.signomix.common.db;

import java.util.Map;

import com.signomix.common.hcms.Document;
import com.signomix.common.news.NewsDefinition;
import com.signomix.common.news.NewsEnvelope;

import io.agroal.api.AgroalDataSource;

public interface NewsDaoIface {

    public void setDatasource(AgroalDataSource dataSource);
    public void createStructure() throws IotDatabaseException;
    public void backupDb() throws IotDatabaseException;
    public long saveNewsDefinition(NewsDefinition news) throws IotDatabaseException;  
    public void saveNewsEnvelope(NewsEnvelope newsEnvelope) throws IotDatabaseException;
    public void saveNewsDocuments(long newsId, Map<String,Document> documents) throws IotDatabaseException;
}
