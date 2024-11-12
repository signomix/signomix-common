package com.signomix.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class SecretsReader {

    /**
     * Read properties from file
     * 
     * @param fileName
     * @return Properties 
     * @throws IOException 
     */
    public static Properties readProperties(String fileName) throws IOException {
        Properties prop = new Properties();
        prop.load(Files.newInputStream(Path.of(fileName)));
        return prop;
    }

    /**
     * Read value from file
     * 
     * @param fileName
     * @return String value
     * @throws IOException 
     */
    public static String readValue(String fileName) throws IOException {
        Path filePath = Path.of(fileName);
        return Files.readString(filePath).trim();
    }

}
