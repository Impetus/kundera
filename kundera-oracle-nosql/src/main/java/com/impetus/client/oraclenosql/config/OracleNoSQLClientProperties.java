/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql.config;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.oraclenosql.OracleNoSQLClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientPropertiesSetter;

/**
 * OracleNoSQL implementation of {@link ClientPropertiesSetter}
 * 
 * @author amresh.singh
 */
public class OracleNoSQLClientProperties
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(OracleNoSQLClientProperties.class);

    private static final String WRITE_TIMEOUT = "write.timeout";

    private static final String DURABILITY = "durability";

    private static final String TIME_UNIT = "time.unit";

    private static final String CONSISTENCY = "consistency";

    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        OracleNoSQLClient oracleNoSQLClient = (OracleNoSQLClient) client;

        if (properties != null)
        {
            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                if (key.equals(WRITE_TIMEOUT) && value instanceof Integer)
                {
                    oracleNoSQLClient.setTimeout((Integer) value);
                }
                else if (key.equals(DURABILITY) && value instanceof Durability)
                {
                    oracleNoSQLClient.setDurability((Durability) value);
                }

                else if (key.equals(TIME_UNIT) && value instanceof TimeUnit)
                {
                    oracleNoSQLClient.setTimeUnit((TimeUnit) value);
                }

                else if (key.equals(CONSISTENCY) && value instanceof Consistency)
                {
                    oracleNoSQLClient.setConsistency((Consistency) value);
                }

                // Add more properties as needed
            }
        }
    }

}
