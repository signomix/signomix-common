package com.signomix.common.gui;

public class ItemResDef {
    public boolean fixed;
    public boolean resizable;
    public boolean draggable;
    public boolean customDragger;
    public boolean customResizer;
    public ItemSize min;
    public ItemSize max;
    public int w;
    public int h;
    public int x;
    public int y;
    public String id;

    public ItemResDef(String id) {
        fixed = false;
        resizable = true;
        draggable = true;
        customDragger = false;
        customResizer = false;
        min = new ItemSize(1,1);
        max = new ItemSize(10, 4);
        w = 1;
        h = 1;
        x = 0;
        y = 0;
        this.id = id;
    }

    public ItemResDef(int x, int y, String id) {
        fixed = false;
        resizable = true;
        draggable = true;
        customDragger = false;
        customResizer = false;
        min = new ItemSize(1,1);
        max = new ItemSize(10, 4);
        w = 1;
        h = 1;
        this.x = x;
        this.y = y;
        this.id = id;
    }

    public void setX(int x) {
        this.x = x;
    }

    public void setY(int y) {
        this.y = y;
    }

    public void setW(int w) {
        this.w = w;
    }

    public void setH(int h) {
        this.h = h;
    }

    public void setId(String id) {
        this.id = id;
    }
}
