package com.impetus.dao.utils;

import java.io.IOException;
import java.io.InputStream;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;

public class JsonUtil
{
    private static Logger LOGGER = LoggerFactory.getLogger(JsonUtil.class);

    public final static <T> T readJson(String json, Class<T> clazz)
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            if (json != null && !json.isEmpty())
            {
                return mapper.readValue(json, clazz);
            }
            else
            {
                LOGGER.error("JSON is null or empty.");
                throw new KunderaException("JSON is null or empty.");
            }
        }
        catch (IOException e)
        {
            LOGGER.error("Error while converting in json{} presentation{}.", json, e);
            throw new KunderaException("Error while mapping JSON to Object. Caused By: ", e);
        }

    }
    
    public final static <T> T readJson(InputStream jsonStream, Class<T> clazz) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            if(jsonStream != null) {
                  return mapper.readValue(jsonStream, clazz);
            }
        } catch (IOException e) {
            LOGGER.error("Error while converting in json{} presentation{}.", jsonStream,e);
            throw new KunderaException("Error while mapping JSON to Object. Caused By: ", e);
        }
 
        return null;
    }

}
