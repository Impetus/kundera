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

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientPropertiesSetter;

/**
 * Cassandra implementation of {@link ClientPropertiesSetter}
 * 
 * @author amresh.singh
 */
class CassandraClientProperties
{

	/**
	 * 
	 */
	private static final String TTL_VALUES = "ttl.values";
	private static final String TTL_PER_REQUEST = "ttl.per.request";
	private static final String TTL_PER_SESSION = "ttl.per.session";

	private static final String CONSISTENCY_LEVEL = "consistency.level";

    private static final String CQL_VERSION = CassandraConstants.CQL_VERSION;

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
                else if (key.equals(CQL_VERSION) && value instanceof String)
                {
                    cassandraClientBase.setCqlVersion((String) value);
                }
                else if(key.equals(TTL_PER_SESSION) && value instanceof Boolean)
                {
                	cassandraClientBase.setTtlPerSession((Boolean)value);
                }
                else if(key.equals(TTL_PER_REQUEST) && value instanceof Boolean)
                {
                	cassandraClientBase.setTtlPerRequest((Boolean)value);
                }
                else if(key.equals(TTL_VALUES) && value instanceof Map)
                {
                	cassandraClientBase.setTtlValues((Map)value);
                }

                // Add more properties as needed
            }
        }
    }
}
