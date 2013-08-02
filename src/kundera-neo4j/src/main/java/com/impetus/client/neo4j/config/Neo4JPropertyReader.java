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
package com.impetus.client.neo4j.config;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;

/**
 * XML Property reader for Neo4J specific configuration
 * 
 * @author amresh.singh
 */
public class Neo4JPropertyReader extends AbstractPropertyReader implements PropertyReader
{

    /** log instance */
    private static Logger log = LoggerFactory.getLogger(Neo4JPropertyReader.class);

    /** Neo4J schema metadata instance */
    public static Neo4JSchemaMetadata nsmd;

    public Neo4JPropertyReader(Map externalProperties)
    {
        super(externalProperties);
        nsmd = new Neo4JSchemaMetadata();
    }

    /**
     * Sets Client properties from XML configuration file into
     * {@link Neo4JSchemaMetadata}
     */
    @Override
    protected void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            nsmd.setClientProperties(cp);
        }
    }

    /**
     * Holds property related to Neo4J specific configuration file
     * 
     * @author Amresh
     * 
     */
    public class Neo4JSchemaMetadata
    {
        private static final String NEO4J_DATASTORE = "neo4j";

        private ClientProperties clientProperties;

        public Neo4JSchemaMetadata()
        {

        }

        /**
         * @return the clientProperties
         */
        public ClientProperties getClientProperties()
        {
            return clientProperties;
        }

        /**
         * @param clientProperties
         *            the clientProperties to set
         */
        private void setClientProperties(ClientProperties clientProperties)
        {
            this.clientProperties = clientProperties;
        }

        /**
         * Returns datastore instance for given {@link ClientProperties} for
         * Neo4j
         * 
         * @return
         */
        public DataStore getDataStore()
        {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null)
            {
                for (DataStore dataStore : getClientProperties().getDatastores())
                {
                    if (dataStore.getName() != null && dataStore.getName().equalsIgnoreCase(NEO4J_DATASTORE))
                    {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }

}
