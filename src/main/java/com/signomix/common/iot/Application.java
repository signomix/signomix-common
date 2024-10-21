package com.signomix.common.iot;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.jboss.logging.Logger;

public class Application {

    private Logger LOG = Logger.getLogger(Application.class);

    public Long id;
    public Long organization;
    public Long version;
    public String name;
    public String description;
    public ApplicationConfig config;

    public Application(Long id, Long organization, Long version, String name, String description) {
        this.id = id;
        this.organization = organization;
        this.version = version;
        this.name = name;
        this.description = description;
        this.config = new ApplicationConfig();
    }

    public Application(Long id, Long organization, Long version, String name, String description,
            ApplicationConfig config) {
        this.id = id;
        this.organization = organization;
        this.version = version;
        this.name = name;
        this.description = description;
        this.config = config;
    }

    public void setConfig(ApplicationConfig config) {
        this.config = config;
    }

    public void setConfig(String applicationConfig) {
        ApplicationConfig mapping;
        try {
            mapping = new ObjectMapper().readValue(applicationConfig, ApplicationConfig.class);
            this.config = mapping;
        } catch (IOException ex) {
            LOG.warn(ex.getMessage());
            this.config = new ApplicationConfig();
        }
    }

    /**
     * Updates configuration by adding new parameters and changing the value of
     * existing parameters.
     * 
     * @param newParameters
     */
    /*
     * public void updateConfigParemeters(HashMap<String, Object> newParameters) {
     * try {
     * Map args1 = new HashMap();
     * args1.put(JsonReader.USE_MAPS, true);
     * JsonReader jr = new JsonReader();
     * Map configurationMap = (Map) JsonReader.jsonToJava(this.description, args1);
     * Iterator<String> it = configurationMap.keySet().iterator();
     * String key;
     * while (it.hasNext()) {
     * key = it.next();
     * configurationMap.put(key, configurationMap.get(key));
     * }
     * it=newParameters.keySet().iterator();
     * while(it.hasNext()){
     * key=it.next();
     * configurationMap.put(key,newParameters.get(key));
     * }
     * Map args2 = new HashMap();
     * args2.put(JsonWriter.TYPE, false);
     * args2.put(JsonWriter.PRETTY_PRINT, true);
     * this.description = JsonWriter.objectToJson(configurationMap, args2);
     * } catch (Exception e) {
     * e.printStackTrace();
     * }
     * }
     */
}
