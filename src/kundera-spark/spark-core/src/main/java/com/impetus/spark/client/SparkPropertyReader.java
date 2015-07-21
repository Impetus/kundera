/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.spark.client;

import java.util.Map;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * The Class SparkPropertyReader.
 * 
 * @author devender.yadav
 */
public class SparkPropertyReader extends AbstractPropertyReader implements PropertyReader
{

    /** The ssmd. */
    public static SparkSchemaMetadata ssmd;

    /**
     * Instantiates a new spark property reader.
     * 
     * @param externalProperties
     *            the external properties
     * @param puMetadata
     *            the pu metadata
     */
    public SparkPropertyReader(Map externalProperties, final PersistenceUnitMetadata puMetadata)
    {
        super(externalProperties, puMetadata);
        ssmd = new SparkSchemaMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.AbstractPropertyReader#onXml(com.impetus
     * .kundera.configure.ClientProperties)
     */
    public void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            ssmd.setClientProperties(cp);
        }
    }

    /**
     * The Class SparkSchemaMetadata.
     */
    public class SparkSchemaMetadata
    {

        /** The client properties. */
        private ClientProperties clientProperties;

        /**
         * Instantiates a new spark schema metadata.
         */
        public SparkSchemaMetadata()
        {

        }

        /**
         * Gets the client properties.
         * 
         * @return the clientProperties
         */
        public ClientProperties getClientProperties()
        {
            return clientProperties;
        }

        /**
         * Sets the client properties.
         * 
         * @param clientProperties
         *            the clientProperties to set
         */
        private void setClientProperties(ClientProperties clientProperties)
        {
            this.clientProperties = clientProperties;
        }

        /**
         * Gets the data store.
         * 
         * @param datastore
         *            the datastore
         * @return the data store
         */
        public DataStore getDataStore(String datastore)
        {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null)
            {
                for (DataStore dataStore : getClientProperties().getDatastores())
                {
                    if (dataStore.getName() != null && dataStore.getName().trim().equalsIgnoreCase(datastore))
                    {
                        return dataStore;
                    }
                }
            }
            return null;
        }

    }
}