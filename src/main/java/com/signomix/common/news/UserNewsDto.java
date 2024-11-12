package com.signomix.common.news;

import java.util.ArrayList;
import java.util.List;

public class UserNewsDto {
    public List<NewsEnvelope> news = new ArrayList<>();
    public int size = 0;
    public String errorMessage = null;
}
