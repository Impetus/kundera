/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu;

import java.util.Map;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * The Class KuduDBPropertyReader.
 * 
 * @author karthikp.manchala
 */
public class KuduDBPropertyReader extends AbstractPropertyReader implements PropertyReader
{

    /** The ksmd. */
    public static KuduDBSchemaMetadata ksmd;

    /**
     * Instantiates a new kudu db property reader.
     * 
     * @param externalProperties
     *            the external properties
     * @param puMetadata
     *            the pu metadata
     */
    public KuduDBPropertyReader(Map externalProperties, final PersistenceUnitMetadata puMetadata)
    {
        super(externalProperties, puMetadata);
        ksmd = new KuduDBSchemaMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.configure.AbstractPropertyReader#onXml(com.impetus.
     * kundera.configure.ClientProperties)
     */
    public void onXml(ClientProperties cp)
    {
        if (cp != null)
        {
            ksmd.setClientProperties(cp);
        }
    }

    /**
     * The Class KuduDBSchemaMetadata.
     */
    public class KuduDBSchemaMetadata
    {

        /** The client properties. */
        private ClientProperties clientProperties;

        /**
         * Instantiates a new kudu db schema metadata.
         */
        public KuduDBSchemaMetadata()
        {

        }

        /**
         * Gets the client properties.
         * 
         * @return the client properties
         */
        public ClientProperties getClientProperties()
        {
            return clientProperties;
        }

        /**
         * Sets the client properties.
         * 
         * @param clientProperties
         *            the new client properties
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