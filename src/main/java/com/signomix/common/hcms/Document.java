package com.signomix.common.hcms;

import java.util.HashMap;

public class Document {

    public String path = "";
    public String name = "";
    public String fileName = "";
    public String content = "";
    public byte[] binaryContent = null;
    public long updateTimestamp = 0;
    public long refreshTimestamp = 0;
    public HashMap<String, String> metadata = new HashMap<>();
    public boolean binaryFile = false;
    public String mediaType = "";
    public String siteName = "";

    public Document() {
    }

    public String getFileName() {
        return name.substring(path.length());
    }

    public Document clone(boolean withContent) {
        Document doc = new Document();
        doc.path = path;
        doc.name = name;
        doc.fileName = fileName.trim();
        doc.updateTimestamp = updateTimestamp;
        doc.metadata = metadata;
        doc.refreshTimestamp = refreshTimestamp;
        doc.binaryFile = binaryFile;
        doc.mediaType = mediaType;
        doc.siteName = getSiteName();
        if (withContent) {
            doc.content = content;
            doc.binaryContent = binaryContent;
        }
        for (String key : metadata.keySet()) {
            doc.metadata.put(key, metadata.get(key));
        }
        return doc;
    }

    public boolean hasProperty(String key) {
        return metadata.containsKey(key);
    }

    public String getProperty(String key) {
        return metadata.get(key);
    }

    public String getSiteName() {
        return siteName;
    }


}
