package com.signomix.common.iot.tts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationIds {

    @JsonProperty("application_id")
    private String applicationId;

    // Gettery i Settery
    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @Override
    public String toString() {
        return "ApplicationIds{" +
                "applicationId='" + applicationId + '\'' +
                '}';
    }
}
