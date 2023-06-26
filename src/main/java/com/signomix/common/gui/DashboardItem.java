package com.signomix.common.gui;

import java.util.UUID;

public class DashboardItem {
    public String id;
    public ItemResDef _el1;
    public ItemResDef _el10;

    public DashboardItem() {
        id = UUID.randomUUID().toString();
        _el1 = new ItemResDef(id);
        _el10 = new ItemResDef(id);
    }

    public DashboardItem(int x1, int y1, int x10, int y10) {
        id = UUID.randomUUID().toString();
        _el1 = new ItemResDef(x1, y1, id);
        _el10 = new ItemResDef(x10, y10, id);
    }
}
