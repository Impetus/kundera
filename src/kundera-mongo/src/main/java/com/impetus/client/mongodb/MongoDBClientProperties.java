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

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.mongodb.DBEncoder;
import com.mongodb.WriteConcern;

/**
 * MongoDB implementation of {@link ClientPropertiesSetter}
 * 
 * @author amresh.singh
 */
public class MongoDBClientProperties
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(MongoDBClientProperties.class);

    public static final String WRITE_CONCERN = "write.concern";

    public static final String DB_ENCODER = "db.encoder";

    public static final String BATCH_SIZE = "batch.size";

    private MongoDBClient mongoDBClient;

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
            this.mongoDBClient.setBatchSize((Integer) value);

        }
        else if (value instanceof String)
        {

            this.mongoDBClient.setBatchSize(Integer.valueOf((String) value));
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
