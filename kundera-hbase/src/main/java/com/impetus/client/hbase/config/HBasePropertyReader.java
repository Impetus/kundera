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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.Compression;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * HBase Property Reader reads hbase properties from property file
 * {kundera-hbase.properties} and put it into hbase schema metadata.
 * 
 * @author kuldeep.mishra
 * 
 */
public class HBasePropertyReader implements PropertyReader
{

    private static Log log = LogFactory.getLog(HBasePropertyReader.class);

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
    @Override
    public void read(String pu)
    {
        Properties properties = new Properties();
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        String propertyName = puMetadata != null ? puMetadata
                .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;

        InputStream inStream = propertyName != null ? ClassLoader.getSystemResourceAsStream(propertyName) : null;
        if (inStream != null)
        {
            try
            {
                properties.load(inStream);
                readProperties(properties);
            }
            catch (IOException e)
            {
                log.warn("error in loading properties , caused by :" + e.getMessage());
                throw new KunderaException(e);
            }
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
        hsmd.setZookeeper_port(properties.getProperty(Constants.ZOOKEEPER_PORT));
        hsmd.setZookeeper_host(properties.getProperty(Constants.ZOOKEEPER_HOST));

        hsmd.addColumnFamilyProperty(properties.getProperty(Constants.CF_DEFS));
    }

    public class HBaseSchemaMetadata
    {
        /**
         * zookeeper port.
         */
        private String zookeeper_port = "2181";

        /**
         * zookeeper host.
         */
        private String zookeeper_host = "localhost";

        /**
         * It holds all property related to columnFamily.
         */
        private Map<String, HBaseColumnFamilyProperties> columnFamilyProperties;

        /**
         * @return the zookeeper_port
         */
        public String getZookeeper_port()
        {
            return zookeeper_port;
        }

        /**
         * @param zookeeper_port
         *            the zookeeper_port to set
         */
        public void setZookeeper_port(String zookeeper_port)
        {
            if (zookeeper_port != null)
            {
                this.zookeeper_port = zookeeper_port;
            }

        }

        /**
         * @return the zookeeper_host
         */
        public String getZookeeper_host()
        {
            return zookeeper_host;
        }

        /**
         * @param zookeeper_host
         *            the zookeeper_host to set
         */
        public void setZookeeper_host(String zookeeper_host)
        {
            if (zookeeper_host != null)
            {
                this.zookeeper_host = zookeeper_host;
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
    }
}
