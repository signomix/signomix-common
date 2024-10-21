package com.signomix.common.iot;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ApplicationConfig extends HashMap<String, String> {
    public List<String> getValueList(String key) {
        String configValue = get(key);
        if (configValue == null) {
            return new ArrayList<>();
        }
        String[] values = configValue.split(",");
        List<String> valueList = new ArrayList<>();
        for (String value : values) {
            valueList.add(value);
        }
        return valueList;
    }

    public String getAsString(){
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            return "";
        }
    }
}

