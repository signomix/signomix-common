package com.signomix.common.api;

import java.util.Map;

import com.signomix.common.event.MessageServiceIface;

public interface ResponseTransformerIface {
    public String transform(String payload, Map<String, Object> options, MessageServiceIface messageService);
    public Map<String, String> getHeaders(Map<String, Object> options, String input, String response);
}
