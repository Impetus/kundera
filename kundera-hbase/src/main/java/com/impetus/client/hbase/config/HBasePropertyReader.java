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

/**
 * HBase Property Reader reads hbase properties from property file
 * {kundera-hbase.properties} and put it into hbase schema metadata.
 * 
 * @author kuldeep.mishra
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

    public HBasePropertyReader(Map externalProperties)
    {
        super(externalProperties);
        hsmd = new HBaseSchemaMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.PropertyReader#read(java.lang.String)
     */

    public void onXml(ClientProperties cp)
    {
        hsmd.onInitialize();
        if (cp != null)
        {
            hsmd.setClientProperties(cp);
        }
    }

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
         * 
         */
        private void onInitialize()
        {
            zookeeperHost = puMetadata != null ? puMetadata.getProperty(PersistenceProperties.KUNDERA_NODES) : null;
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
         * @return the zookeeper_port
         */
        public String getZookeeperPort()
        {
            DataStore ds = getDataStore();
            if (ds != null && ds.getConnection() != null)
            {
                Connection conn = ds.getConnection();
                if (conn.getProperties() != null && !conn.getProperties().isEmpty())
                {
                    zookeeperPort = conn.getProperties().getProperty(HBaseConstants.ZOOKEEPER_PORT);
                }
            }
            return zookeeperPort;
        }

        /**
         * @return the zookeeper_host
         */
        public String getZookeeperHost()
        {
            DataStore ds = getDataStore();
            if (ds != null && ds.getConnection() != null)
            {
                Connection conn = ds.getConnection();
                if (conn.getProperties() != null && !conn.getProperties().isEmpty())
                {
                    zookeeperHost = conn.getProperties().getProperty(HBaseConstants.ZOOKEEPER_HOST);
                }
            }
            return zookeeperHost;
        }

        public DataStore getDataStore()
        {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null)
            {
                for (DataStore dataStore : getClientProperties().getDatastores())
                {
                    if (dataStore.getName() != null && dataStore.getName().equalsIgnoreCase("hbase"))
                    {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }
}
