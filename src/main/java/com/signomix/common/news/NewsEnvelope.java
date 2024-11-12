package com.signomix.common.news;

import java.sql.Timestamp;

public class NewsEnvelope {
    public Long id;
    public Long newsId;
    public Timestamp read;
    public Timestamp created;
    public String userId;
    public String language;
    public boolean pinned; // if true, the news will be shown on top of the list
    public String title;
}
