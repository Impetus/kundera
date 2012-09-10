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
package com.impetus.client.cassandra;

import java.util.Map;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientPropertiesSetter;

/**
 * Cassandra implementation of {@link ClientPropertiesSetter} 
 * @author amresh.singh
 */
class CassandraClientProperties
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(CassandraClientProperties.class);
    
    private static final String CONSISTENCY_LEVEL = "cassandra.consistency.level";
    private static final String CQL_VERSION = "cassandra.cql.version";
    

    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        CassandraClientBase cassandraClientBase = (CassandraClientBase) client;
        
        if (properties != null)
        {
            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                if (key.equals(CONSISTENCY_LEVEL) && value instanceof ConsistencyLevel)
                {
                    cassandraClientBase.setConsistencyLevel((ConsistencyLevel) value);
                }
                else if(key.equals(CQL_VERSION) && value instanceof String)
                {
                    cassandraClientBase.setCqlVersion((String) value);
                }

                // Add more properties as needed

                
            }
        }
        
        
    }

}
