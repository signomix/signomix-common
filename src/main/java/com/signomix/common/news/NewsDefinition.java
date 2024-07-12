package com.signomix.common.news;

import java.sql.Timestamp;
import java.util.Map;

public class NewsDefinition {
    public Long id;
    public String name;
    public Map<String, String> documents;
    public String userId;
    public String organizationId;
    public Long organization;
    public Long tenant;
    public String target;
    public String type;
    public Timestamp createdAt;
    public Timestamp plannedAt;
    public Timestamp publishedAt;

}
