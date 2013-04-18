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
package com.impetus.client.oraclenosql;

/**
 * Utility class for handling read/ write of data from/ to Oracle NoSQL database
 * @author amresh.singh
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;


/**
 * Provides utility methods for handling data held in Oracle NoSQL KVstore.
 * @author amresh.singh
 */
public class OracleNoSQLDataHandler
{
    
    /** The client. */
    private Client client;

    /** The persistence unit. */
    private String persistenceUnit;

    /** The log. */
    private static Log log = LogFactory.getLog(OracleNoSQLDataHandler.class);
    
    
    /**
     * Instantiates a new mongo db data handler.
     *
     * @param client the client
     * @param persistenceUnit the persistence unit
     */
    public OracleNoSQLDataHandler(Client client, String persistenceUnit)
    {
        super();
        this.client = client;
        this.persistenceUnit = persistenceUnit;
    }

    

    
}