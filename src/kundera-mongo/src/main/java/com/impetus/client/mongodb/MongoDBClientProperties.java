/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.mongodb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.mongodb.DBEncoder;
import com.mongodb.WriteConcern;

/**
 * MongoDB implementation of {@link ClientPropertiesSetter}.
 * 
 * @author amresh.singh
 */
public class MongoDBClientProperties
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(MongoDBClientProperties.class);

    /** The Constant WRITE_CONCERN. */
    public static final String WRITE_CONCERN = "write.concern";

    /** The Constant DB_ENCODER. */
    public static final String DB_ENCODER = "db.encoder";

    /** The Constant BATCH_SIZE. */
    public static final String BATCH_SIZE = "batch.size";

    /** The Constant ORDERED_BULK_OPERATION. */
    public static final String ORDERED_BULK_OPERATION = "ordered.bulk.operation";

    /** The mongo db client. */
    private MongoDBClient mongoDBClient;

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
        this.mongoDBClient = (MongoDBClient) client;

        if (properties != null)
        {
            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                if (checkNull(key, value))
                {
                    if (key.equals(WRITE_CONCERN) && value instanceof WriteConcern)
                    {
                        this.mongoDBClient.setWriteConcern((WriteConcern) value);
                    }
                    else if (key.equals(DB_ENCODER) && value instanceof DBEncoder)
                    {
                        this.mongoDBClient.setEncoder((DBEncoder) value);
                    }
                    else if (key.equals(BATCH_SIZE))
                    {
                        setBatchSize(value);
                    }
                    else if (key.equals(ORDERED_BULK_OPERATION))
                    {
                        try
                        {
                            this.mongoDBClient.setOrderedBulkOperation((boolean) value);
                        }
                        catch (ClassCastException ex)
                        {
                            log.error(
                                    "only boolean value is supported for ORDERED_BULK_OPERATION property. Caused By: ",
                                    ex);
                            throw new KunderaException(
                                    "only boolean value is supported for ORDERED_BULK_OPERATION property. Caused By: ",
                                    ex);

                        }
                    }
                }
                // Add more properties as needed
            }
        }
    }

    /**
     * set batch size.
     * 
     * @param value
     *            the new batch size
     */
    private void setBatchSize(Object value)
    {
        if (value instanceof Integer)
        {
            this.mongoDBClient.setBatchSize((Integer) value);
        }
        else if (value instanceof String)
        {
            this.mongoDBClient.setBatchSize(Integer.valueOf((String) value));
        }
    }

    /**
     * check key value map not null.
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
