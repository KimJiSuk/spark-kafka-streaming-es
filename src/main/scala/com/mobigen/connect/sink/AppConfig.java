package com.mobigen.connect.sink;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private static final Logger LOGGER = Logger.getLogger(AppConfig.class);

    private static Properties _properties;
    private static Object _lock = new Object();

    public static Properties loadProperties()
    {
        InputStream is = AppConfig.class.getClassLoader().getResourceAsStream("application.properties");

        synchronized (_lock)
        {
            if (_properties == null)
            {
                _properties = new Properties();
                try
                {
                    _properties.load(is);
                    LOGGER.info("Properties loaded");
                }
                catch (IOException e)
                {
                    LOGGER.info("Failed to load properties", e);
                    System.exit(1);
                }
            }
            return _properties;
        }
    }
}
