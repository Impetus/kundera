/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.dao.utils;

import java.io.IOException;
import java.io.InputStream;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;

/**
 * The Class JsonUtil.
 */
public class JsonUtil
{

    /** The logger. */
    private static Logger LOGGER = LoggerFactory.getLogger(JsonUtil.class);

    /**
     * Read json.
     *
     * @param <T>
     *            the generic type
     * @param json
     *            the json
     * @param clazz
     *            the clazz
     * @return the t
     */
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

    /**
     * Read json.
     *
     * @param <T>
     *            the generic type
     * @param jsonStream
     *            the json stream
     * @param clazz
     *            the clazz
     * @return the t
     */
    public final static <T> T readJson(InputStream jsonStream, Class<T> clazz)
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {
            if (jsonStream != null)
            {
                return mapper.readValue(jsonStream, clazz);
            }
            else
            {
                LOGGER.error("InputStream is null.");
                throw new KunderaException("InputStream is null.");
            }
        }
        catch (IOException e)
        {
            LOGGER.error("Error while mapping input stream to object. Caused By: ", e);
            throw new KunderaException("Error while mapping input stream to object. Caused By: ", e);
        }

    }

}
