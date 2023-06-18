package com.signomix.common.gui;

import java.util.UUID;

public class DashboardItem {
    public String id;
    public ItemResDef _el1;
    public ItemResDef _el10;

    public DashboardItem() {
        id = UUID.randomUUID().toString();
        _el1 = new ItemResDef(null);
        _el10 = new ItemResDef(id);
    }
}
