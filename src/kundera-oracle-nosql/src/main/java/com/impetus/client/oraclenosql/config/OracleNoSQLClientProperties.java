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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.oraclenosql.OracleNoSQLClient;
import com.impetus.kundera.PersistenceProperties;
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

    private OracleNoSQLClient oracleNoSQLClient;

    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        this.oracleNoSQLClient = (OracleNoSQLClient) client;

        if (properties != null)
        {
            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                if (checkNull(key, value))
                {
                    if (key.equals(WRITE_TIMEOUT))
                    {
                        setTimeOut(value);

                    }
                    else if (key.equals(DURABILITY) && value instanceof Durability)
                    {

                        this.oracleNoSQLClient.setDurability((Durability) value);
                    }

                    else if (key.equals(TIME_UNIT))
                    {
                        setTimeUnit(value);

                    }

                    else if (key.equals(CONSISTENCY) && value instanceof Consistency)
                    {
                        this.oracleNoSQLClient.setConsistency((Consistency) value);
                    }
                    else if (key.equals(PersistenceProperties.KUNDERA_BATCH_SIZE))
                    {
                        setBatchSize(value);

                    }

                    // Add more properties as needed
                }
            }
        }
    }

    /**
     * set time out
     */
    private void setTimeOut(Object value)
    {
        if (value instanceof Integer)
        {
            this.oracleNoSQLClient.setTimeout((Integer) value);

        }
        else if (value instanceof String)
        {

            this.oracleNoSQLClient.setTimeout(Integer.valueOf((String) value));
        }

    }

    /**
     * set time unit
     */
    private void setTimeUnit(Object value)
    {
        if (value instanceof TimeUnit)
        {
            this.oracleNoSQLClient.setTimeUnit((TimeUnit) value);
        }
        else if (value instanceof String)
        {
            this.oracleNoSQLClient.setTimeUnit(TimeUnit.valueOf((String) value));
        }

    }

    /**
     * set batch size
     */
    private void setBatchSize(Object value)
    {
        if (value instanceof Integer)
        {
            this.oracleNoSQLClient.setBatchSize((Integer) value);
        }
        else if (value instanceof String)
        {
            this.oracleNoSQLClient.setBatchSize(Integer.valueOf((String) value));
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
