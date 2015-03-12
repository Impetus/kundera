/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase;

import java.util.Map;

import org.apache.hadoop.hbase.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;

/**
 * The Class HBaseClientProperties.
 */
/**
 * @author Devender Yadav
 * 
 */
class HBaseClientProperties
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(HBaseClientProperties.class);

    private static final String FILTER = "hbase.filter";

    private HBaseClient hbaseClient;

    /**
     * Populate client properties.
     * 
     * @param client
     *            the client
     * @param properties
     *            the properties
     */
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        this.hbaseClient = (HBaseClient) client;

        if (properties != null)
        {

            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                if (checkNull(key, value))
                {
                    if (key.equals(FILTER) && value instanceof Filter)
                    {
                        this.hbaseClient.setFilter((Filter) value);
                    }
                    else if (key.equals(PersistenceProperties.KUNDERA_BATCH_SIZE))
                    {
                        setBatchSize(value);
                    }
                }
            }
        }
    }

    /**
     * Sets the batch size.
     * 
     * @param value
     *            the new batch size
     */
    private void setBatchSize(Object value)
    {
        if (value instanceof Integer)
        {
            this.hbaseClient.setBatchSize((Integer) value);
        }
        else if (value instanceof String)
        {
            this.hbaseClient.setBatchSize(Integer.valueOf((String) value));
        }

    }

    /**
     * Check null.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     * @return true, if successful
     */
    private boolean checkNull(String key, Object value)
    {
        return key != null && value != null;
    }
}
