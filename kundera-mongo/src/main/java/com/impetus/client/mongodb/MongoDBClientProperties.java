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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.mongodb.DBEncoder;
import com.mongodb.WriteConcern;

/**
 * MongoDB implementation of {@link ClientPropertiesSetter} 
 * @author amresh.singh
 */
public class MongoDBClientProperties
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(MongoDBClientProperties.class);
    
    public static final String WRITE_CONCERN = "write.concern";
    public static final String DB_ENCODER = "db.encoder";
    public static final String BATCH_SIZE = "batch.size";
    

    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        MongoDBClient mongoDBClient = (MongoDBClient) client;
        
        if (properties != null)
        {
            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                
                if (key.equals(WRITE_CONCERN) && value instanceof WriteConcern)
                {
                    mongoDBClient.setWriteConcern((WriteConcern) value);
                }
                else if(key.equals(DB_ENCODER) && value instanceof DBEncoder)
                {
                    mongoDBClient.setEncoder((DBEncoder) value);
                }
                else if(key.equals(BATCH_SIZE) && value instanceof Integer)
                {
                    Integer batchSize = (Integer) value;
                    mongoDBClient.setBatchSize(batchSize);
                }

                // Add more properties as needed
                
            }
        }
        
        
    }

}
