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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;


/**
 * XML Property reader for Neo4J specific configuration 
 * @author amresh.singh
 */
public class Neo4JPropertyReader extends AbstractPropertyReader implements PropertyReader
{

    /** log instance */
    private static Log log = LogFactory.getLog(Neo4JPropertyReader.class);

    /** Neo4J schema metadata instance */
    public static Neo4JSchemaMetadata nsmd;
    
    public Neo4JPropertyReader()
    {
        nsmd = new Neo4JSchemaMetadata();
    } 
    

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

        public DataStore getDataStore()
        {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null)
            {
                for (DataStore dataStore : getClientProperties().getDatastores())
                {
                    if (dataStore.getName() != null && dataStore.getName().equalsIgnoreCase("neo4j"))
                    {
                        return dataStore;
                    }
                }
            }
            return null;
        }       
    }

}
