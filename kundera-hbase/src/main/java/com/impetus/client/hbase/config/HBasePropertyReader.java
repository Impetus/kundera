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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.Compression;

import com.impetus.client.hbase.HBaseConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
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
    private static Log log = LogFactory.getLog(HBasePropertyReader.class);

    /**
     * The Hbase schema metadata instance.
     */
    public static HBaseSchemaMetadata hsmd;

    public HBasePropertyReader()
    {
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

    public void onProperties(Properties properties)
    {
        hsmd.onInitialize();
        if (properties != null)
        {
            log.warn("Use of properties file is Deprecated ,please use xml file instead ");
            readProperties(properties);
        }
        else
        {
            log.warn("No property file found in class path, kundera will use default property");
        }
    }

    /**
     * read all properties
     * 
     * @param properties
     */
    private void readProperties(Properties properties)
    {
        hsmd.setZookeeperPort(properties.getProperty(HBaseConstants.ZOOKEEPER_PORT));
        hsmd.setZookeeperHost(properties.getProperty(HBaseConstants.ZOOKEEPER_HOST));

        hsmd.addColumnFamilyProperty(properties.getProperty(HBaseConstants.CF_DEFS));
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
         * It holds all property related to columnFamily.
         */
        private Map<String, HBaseColumnFamilyProperties> columnFamilyProperties;

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
            return zookeeperPort;
        }

        /**
         * @param zookeeper_port
         *            the zookeeper_port to set
         */
        public void setZookeeperPort(String zookeeperPort)
        {
            if (zookeeperPort != null)
            {
                this.zookeeperPort = zookeeperPort;
            }

        }

        /**
         * @return the zookeeper_host
         */
        public String getZookeeperHost()
        {
            return zookeeperHost;
        }

        /**
         * @param zookeeper_host
         *            the zookeeper_host to set
         */
        public void setZookeeperHost(String zookeeperHost)
        {
            if (zookeeperHost != null)
            {
                this.zookeeperHost = zookeeperHost;
            }

        }

        /**
         * @return the columnFamilyProperties
         */
        public Map<String, HBaseColumnFamilyProperties> getColumnFamilyProperties()
        {
            if (columnFamilyProperties == null)
            {
                columnFamilyProperties = new HashMap<String, HBaseColumnFamilyProperties>();
            }

            return columnFamilyProperties;
        }

        public void addColumnFamilyProperty(String cfDefs)
        {
            if (cfDefs != null)
            {
                HBaseColumnFamilyProperties familyProperties = new HBaseColumnFamilyProperties();
                StringTokenizer cfDef = new StringTokenizer(cfDefs, ",");
                String[] tokenNames = { "tableName", "algo", "ttl", "maxVer", "minVer" };
                Map<String, String> tokens = new HashMap<String, String>();
                while (cfDef.hasMoreTokens())
                {
                    StringTokenizer tokenizer = new StringTokenizer(cfDef.nextToken(), "|");
                    int count = 0;
                    while (tokenizer.hasMoreTokens())
                    {
                        tokens.put(tokenNames[count++], tokenizer.nextToken());
                    }
                }
                String algoName = tokens.get(tokenNames[1]);
                Compression.Algorithm algo = null;
                try
                {
                    algo = Compression.Algorithm.valueOf(algoName);
                }
                catch (IllegalArgumentException iae)
                {
                    log.warn("given compression algorithm is not valid, kundera will use default compression algorithm");
                }

                familyProperties.setAlgorithm(algoName != null ? algo : null);
                familyProperties.setTtl(tokens.get(tokenNames[2]));
                familyProperties.setMaxVersion(tokens.get(tokenNames[3]));
                familyProperties.setMinVersion(tokens.get(tokenNames[4]));
                getColumnFamilyProperties().put(tokens.get(tokenNames[0]), familyProperties);
            }
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
