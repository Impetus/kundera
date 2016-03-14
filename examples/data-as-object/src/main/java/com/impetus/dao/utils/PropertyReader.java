package com.impetus.dao.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;

public class PropertyReader
{
    private static Logger LOGGER = LoggerFactory.getLogger(PropertyReader.class);
    
    public static Properties getProps(String fileName) throws Exception
    {
        Properties properties = new Properties();
        InputStream inputStream = PropertyReader.class.getClassLoader().getResourceAsStream(fileName);

        if (inputStream != null)
        {
            try
            {
                properties.load(inputStream);
            }
            catch (IOException e)
            {
                LOGGER.error("JSON is null or empty.");
                throw new KunderaException("JSON is null or empty.");
            }
        }
        else
        {
            throw new KunderaException("Property file: [" + fileName + "] not found in the classpath");
        }
        return properties;
    }

}
