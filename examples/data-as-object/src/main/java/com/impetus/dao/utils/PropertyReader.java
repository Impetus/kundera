package com.impetus.dao.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyReader
{
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
                e.printStackTrace();
            }
        }
        else
        {
            throw new Exception("Property file: [" + fileName + "] not found in the classpath");
        }
        return properties;
    }

}
