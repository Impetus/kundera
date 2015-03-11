/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.config;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.HBaseConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.ClientProperties.DataStore.Connection;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * @author Devender Yadav
 *
 */
public class HBasePropertyReader extends AbstractPropertyReader implements PropertyReader
{

    /**
     * The log instance.
     */
    private static Logger log = LoggerFactory.getLogger(HBasePropertyReader.class);

    /**
     * The Hbase schema metadata instance.
     */
    public static HBaseSchemaMetadata hsmd;

    /**
     * Instantiates a new h base property reader.
     * 
     * @param externalProperties
     *            the external properties
     * @param puMetadata
     *            the pu metadata
     */
    public HBasePropertyReader(Map externalProperties, final PersistenceUnitMetadata puMetadata)
    {
        super(externalProperties, puMetadata);
        hsmd = new HBaseSchemaMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.PropertyReader#read(java.lang.String)
     */

    @Override
    public void onXml(ClientProperties cp)
    {
        hsmd.onInitialize();
        if (cp != null)
        {
            hsmd.setClientProperties(cp);
        }
    }

    /**
     * The Class HBaseSchemaMetadata.
     */
    public class HBaseSchemaMetadata
    {
        /**
         * zookeeper port.
         */
        private String zookeeperPort = "2181";

        /**
         * zookeeper host.
         */
        private String zookeeperHost;

        /**
         * client properties.
         */
        private ClientProperties clientProperties;

        /**
         * On initialize.
         */
        private void onInitialize()
        {
            zookeeperHost = puMetadata != null ? puMetadata.getProperty(PersistenceProperties.KUNDERA_NODES) : null;
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
         * Gets the zookeeper port.
         * 
         * @return the zookeeper port
         */
        public String getZookeeperPort()
        {
            DataStore ds = getDataStore();
            if (ds != null && ds.getConnection() != null)
            {
                Connection conn = ds.getConnection();
                if (conn.getProperties() != null && !conn.getProperties().isEmpty())
                {
                    zookeeperPort = conn.getProperties().getProperty(HBaseConstants.ZOOKEEPER_PORT).trim();
                }
            }
            return zookeeperPort;
        }

        /**
         * Gets the zookeeper host.
         * 
         * @return the zookeeper host
         */
        public String getZookeeperHost()
        {
            DataStore ds = getDataStore();
            if (ds != null && ds.getConnection() != null)
            {
                Connection conn = ds.getConnection();
                if (conn.getProperties() != null && !conn.getProperties().isEmpty())
                {
                    zookeeperHost = conn.getProperties().getProperty(HBaseConstants.ZOOKEEPER_HOST.trim());
                }
            }
            return zookeeperHost;
        }

        /**
         * Gets the data store.
         * 
         * @return the data store
         */
        public DataStore getDataStore()
        {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null)
            {
                for (DataStore dataStore : getClientProperties().getDatastores())
                {
                    if (dataStore.getName() != null && dataStore.getName().trim().equalsIgnoreCase("hbase"))
                    {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }
}
