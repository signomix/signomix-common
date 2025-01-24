package com.signomix.common.api;

import java.util.List;
import java.util.Map;

public interface PayloadParserIface {
    /**
     * Parse a payload into a list of maps. Each map contains the name and value of a measure.
     * @param payload
     * @param options
     * @return
     */
    public List<Map> parse(String payload, Map options);
    /**
     * Parse a batch of measures from a payload. The payload is a string 
     * containing measurements from different sensors and different time points.
     * @param payload
     * @param options
     * @return List of MeasureDto objects
     */
    public List<MeasureDto> parseBatch(String payload, Map options);
}
