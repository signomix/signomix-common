package com.signomix.common.proprietary;

import java.lang.reflect.InvocationTargetException;
import org.jboss.logging.Logger;

public class ExtensionConfig {
    private static final Logger logger = Logger.getLogger(ExtensionConfig.class);

    public static Object getExtension(String classPath) {
        if (classPath == null) {
            return null;
        }
        Object extensionInstance = null;
        try {
            extensionInstance = Class.forName(classPath).getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
            logger.warn("Extension "+classPath+" not available", e);
        }
        return extensionInstance;
    }
}
