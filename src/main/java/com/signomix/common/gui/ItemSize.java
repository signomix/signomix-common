package com.signomix.common.gui;

public class ItemSize {
    public int w;
    public int h;

    public ItemSize() {
        w = 1;
        h = 1;
    }

    public ItemSize(int w, int h) {
        this.w = w;
        this.h = h;
    }

    public void setW(int w) {
        this.w = w;
    }

    public void setH(int h) {
        this.h = h;
    }
}
