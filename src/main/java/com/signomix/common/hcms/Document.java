package com.signomix.common.hcms;

import java.util.HashMap;

public class Document {

    public String path="";
    public String name="";
    public String fileName="";
    public String content="";
    public byte[] binaryContent=null;
    public long updateTimestamp=0;
    public HashMap<String, String> metadata = new HashMap<>();
    public boolean binaryFile=false;
    public String mediaType="";
    public Document() {
    } 

    public String getFileName() {
        return name.substring(path.length());
    }

    public Document clone(boolean withContent){
        Document doc = new Document();
        doc.path = path;
        doc.name = name;
        doc.updateTimestamp = updateTimestamp;
        doc.metadata = metadata;
        doc.binaryFile = binaryFile;
        doc.mediaType = mediaType;
        if(withContent){
            doc.content = content;
            doc.binaryContent = binaryContent;
        }
        return doc;
    }

}
