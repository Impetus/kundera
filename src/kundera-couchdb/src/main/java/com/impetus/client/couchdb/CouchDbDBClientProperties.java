/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.couchdb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;

/**
 * 
 * @author Kuldeep Mishra
 *
 */
public class CouchDbDBClientProperties
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(CouchDbDBClientProperties.class);

    public static final String BATCH_SIZE = "batch.size";

    private CouchDBClient couchDBClient;

    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        this.couchDBClient = (CouchDBClient) client;

        if (properties != null)
        {
            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                if (checkNull(key, value))
                {
                    if (key.equals(BATCH_SIZE))
                    {
                        setBatchSize(value);

                    }
                }
                // Add more properties as needed
            }
        }
    }

    /**
     * set batch size
     */
    private void setBatchSize(Object value)
    {
        if (value instanceof Integer)
        {
            this.couchDBClient.setBatchSize((Integer) value);

        }
        else if (value instanceof String)
        {

            this.couchDBClient.setBatchSize(Integer.valueOf((String) value));
        }
    }

    /**
     * check key value map not null
     */
    private boolean checkNull(String key, Object value)
    {
        return key != null && value != null;
    }
}