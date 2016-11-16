package com.anthonykim.datamgr.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hyuk0 on 2016-10-27.
 */
public class PropertiesUtil {
    public static Properties getDefaultProperties(String propertiesFileName) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null)
            classLoader = ClassLoader.getSystemClassLoader();

        Properties properties = new Properties();

        try (InputStream propertiesStream = classLoader.getResourceAsStream(propertiesFileName)) {
            properties.load(propertiesStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public static Properties getExternalProperties(String propertiesFileName) {
        File propertiesFile = new File(propertiesFileName);
        if (!propertiesFile.exists())
            return null;

        Properties properties = new Properties();

        try (InputStream propertiesStream = new FileInputStream(propertiesFileName)) {
            properties.load(propertiesStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}