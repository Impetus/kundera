/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.mongodb.config;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.mongodb.MongoDBConstants;
import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema;
import com.impetus.kundera.configure.ClientProperties.DataStore.Schema.Table;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.configure.schema.SchemaGenerationException;

/**
 * Mongo Property Reader reads mongo properties from property file
 * {kundera-mongo.properties} and put it into mongo schema metadata.
 * 
 * @author kuldeep.mishra
 * 
 */
public class MongoDBPropertyReader extends AbstractPropertyReader implements PropertyReader
{
    /** log instance */
    private static Logger log = LoggerFactory.getLogger(MongoDBPropertyReader.class);

    /** MongoDB schema metadata instance */
    public static MongoDBSchemaMetadata msmd;

    public MongoDBPropertyReader()
    {
        msmd = new MongoDBSchemaMetadata();
    }

    public void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            msmd.setClientProperties(cp);
        }
    }

    /**
     * MongoDBSchemaMetadata class holds property related to metadata
     * 
     * @author kuldeep.mishra
     * 
     */
    public class MongoDBSchemaMetadata
    {
        private ClientProperties clientProperties;

        public MongoDBSchemaMetadata()
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
                    if (dataStore.getName() != null && dataStore.getName().equalsIgnoreCase("mongo"))
                    {
                        return dataStore;
                    }
                }
            }
            return null;
        }

        /**
         * @param databaseName
         * @param tableName
         * @return
         */
        public boolean isCappedCollection(String databaseName, String tableName)
        {
            List<Schema> schemas = getDataStore() != null ? getDataStore().getSchemas() : null;
            if (schemas != null)
            {
                for (Schema schema : schemas)
                {
                    if (schema != null && schema.getName() != null && schema.getName().equalsIgnoreCase(databaseName))
                    {
                        for (Table table : schema.getTables())
                        {
                            if (table.getProperties() != null && tableName.equals(table.getName()))
                            {
                                return Boolean.parseBoolean(table.getProperties().getProperty(MongoDBConstants.CAPPED));
                            }
                        }
                    }
                }
            }
            return false;
        }

        /**
         * @param databaseName
         * @param tableName
         * @return
         */
        public int getCollectionSize(String databaseName, String tableName)
        {
            List<Schema> schemas = getDataStore() != null ? getDataStore().getSchemas() : null;
            if (schemas != null)
            {
                for (Schema schema : schemas)
                {
                    if (schema != null && schema.getName() != null && schema.getName().equalsIgnoreCase(databaseName))
                    {
                        for (Table table : schema.getTables())
                        {
                            if (table.getProperties() != null)
                            {
                                try
                                {
                                    String size = table.getProperties().getProperty(MongoDBConstants.SIZE);
                                    if (size != null && !size.isEmpty())
                                    {
                                        return Integer.parseInt(size);
                                    }
                                }
                                catch (NumberFormatException nfe)
                                {
                                    throw new SchemaGenerationException(nfe);
                                }
                            }
                        }
                    }
                }
            }
            return 100000;
        }

        /**
         * @param databaseName
         * @param tableName
         * @return
         */
        public int getMaxSize(String databaseName, String tableName)
        {
            List<Schema> schemas = getDataStore() != null ? getDataStore().getSchemas() : null;
            if (schemas != null)
            {
                for (Schema schema : schemas)
                {
                    if (schema != null && schema.getName() != null && schema.getName().equalsIgnoreCase(databaseName))
                    {
                        for (Table table : schema.getTables())
                        {
                            if (table.getProperties() != null)
                            {
                                try
                                {
                                    String max = table.getProperties().getProperty(MongoDBConstants.MAX);
                                    if (max != null && !max.isEmpty())
                                    {
                                        return Integer.parseInt(max);
                                    }
                                }
                                catch (NumberFormatException nfe)
                                {
                                    throw new SchemaGenerationException(nfe);
                                }
                            }
                        }
                    }
                }
            }
            return 100;
        }
    }
}
