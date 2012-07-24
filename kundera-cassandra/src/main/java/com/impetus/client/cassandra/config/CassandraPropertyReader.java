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
package com.impetus.client.cassandra.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.cassandra.schemamanager.CassandraValidationClassMapper;
import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Cassandra Property Reader reads cassandra properties from property file
 * {kundera-cassandra.properties} and put it into cassandra schema metadata.
 * 
 * @author kuldeep.mishra
 * 
 */
public class CassandraPropertyReader implements PropertyReader
{
    /** The log instance. */
    private Log log = LogFactory.getLog(CassandraPropertyReader.class);

    /** csmd instance of CassandraSchemaMetadata */
    // private CassandraSchemaMetadata csmd =
    // CassandraValidationClassMapper.getCSMetadata();;

    public static CassandraSchemaMetadata csmd;

    public CassandraPropertyReader()
    {
        csmd = new CassandraSchemaMetadata();
    }

    @Override
    public void read(String pu)
    {
        Properties properties = new Properties();
        try
        {
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
            String propertyName = puMetadata != null ? puMetadata
                    .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY) : null;

            InputStream inStream = propertyName != null ? ClassLoader.getSystemResourceAsStream(propertyName) : null;
            
            if(inStream == null) {
                inStream = this.getClass().getClassLoader().getResourceAsStream(propertyName);  
            }
            if (inStream != null)
            {
                properties.load(inStream);
                readKeyspaceSpecificProprerties(properties);
                readColumnFamilySpecificProperties(properties);
            }
            else
            {
                log.info("No property file found in class path, kundera will use default property");
            }
        }
        catch (IOException e)
        {
            log.warn("error in loading properties , caused by :" + e.getMessage());
            throw new KunderaException(e);
        }
    }

    /**
     * loads column family specific properties and put into map
     * 
     * @param properties
     * @param inStream
     * @throws IOException
     */
    private void readColumnFamilySpecificProperties(Properties properties)
    {
        String cf_defs = properties.getProperty(Constants.CF_DEFS);
        csmd.addCf_defs(cf_defs);
    }

    /**
     * loads keyspace specific properties
     * 
     * @param properties
     * @param inStream
     * @throws IOException
     */
    private void readKeyspaceSpecificProprerties(Properties properties)
    {

        String placementStrategy = properties.getProperty(Constants.PLACEMENT_STRATEGY);
        csmd.setPlacement_strategy(placementStrategy);

        if (csmd.getPlacement_strategy().equalsIgnoreCase(SimpleStrategy.class.getName()))
        {
            String replicationFactor = properties.getProperty(Constants.REPLICATION_FACTOR);
            csmd.setReplication_factor(replicationFactor);
        }
        else
        {
            String dataCenters = properties.getProperty(Constants.DATA_CENTERS);
            csmd.addDataCenter(dataCenters);
        }

        String invertedIndexingEnabled = properties.getProperty(Constants.INVERTED_INDEXING_ENABLED);
        if (invertedIndexingEnabled != null)
        {
            if ("true".equalsIgnoreCase(invertedIndexingEnabled))
            {
                csmd.setInvertedIndexingEnabled(true);
            }
        }
    }

    /**
     * Cassandra schema metadata holds metadata information.
     * 
     * @author kuldeep.mishra
     * 
     */
    public class CassandraSchemaMetadata
    {
        /**
         * It holds all property related to columnFamily.
         */
        private Map<String, CassandraColumnFamilyProperties> columnFamilyProperties;

        /**
         * replication_factor will use in keyspace creation;
         */
        private String replication_factor = "1";

        /**
         * placement_strategy will use in keyspace creation;
         */
        private String placement_strategy = SimpleStrategy.class.getName();

        /** Whether Inverted Indexing is enabled */
        private boolean invertedIndexingEnabled;

        /**
         * dataCenterToNode map holds information about no of node per data
         * center.
         */
        private Map<String, String> dataCentersInfo;

        /**
         * @return the familyToProperties
         */
        public Map<String, CassandraColumnFamilyProperties> getColumnFamilyProperties()
        {
            if (columnFamilyProperties == null)
            {
                columnFamilyProperties = new HashMap<String, CassandraColumnFamilyProperties>();
            }
            return columnFamilyProperties;
        }

        public void addCf_defs(String cf_defs)
        {
            if (cf_defs != null)
            {
                StringTokenizer cf_def = new StringTokenizer(cf_defs, ",");
                while (cf_def.hasMoreTokens())
                {
                    CassandraColumnFamilyProperties familyProperties = new CassandraColumnFamilyProperties();
                    StringTokenizer tokenizer = new StringTokenizer(cf_def.nextToken(), "|");
                    if (tokenizer.countTokens() != 0 && tokenizer.countTokens() >= 2)
                    {
                        String columnFamilyName = tokenizer.nextToken();
                        String defaultValidationClass = tokenizer.nextToken();
                        if (validate(defaultValidationClass))
                            familyProperties.setDefault_validation_class(defaultValidationClass);
                        if (tokenizer.countTokens() != 0)
                        {
                            String comparator = tokenizer.nextToken();
                            if (!comparator.equalsIgnoreCase(CounterColumnType.class.getSimpleName())
                                    && validate(comparator))
                            {
                                familyProperties.setComparator(comparator);
                            }
                        }
                        getColumnFamilyProperties().put(columnFamilyName, familyProperties);
                    }
                }
            }
        }

        /**
         * validates validators and comparators given by user in property file.
         * 
         * @param args
         * @return
         */
        private boolean validate(String args)
        {
            boolean isValid = false;
            if (CassandraValidationClassMapper.getValidatorsAndComparators().contains(args))
            {
                isValid = true;
                return isValid;
            }
            else
            {
                log.warn("please provide valid default_validation_class and comparators ");
                return isValid;
            }
        }

        /**
         * @return the replication_factor
         */
        public String getReplication_factor()
        {
            return replication_factor;
        }

        /**
         * @param replication_factor
         *            the replication_factor to set
         */
        public void setReplication_factor(String replication_factor)
        {
            if (replication_factor != null)
                this.replication_factor = replication_factor;
        }

        /**
         * @return the placement_strategy
         */
        public String getPlacement_strategy()
        {
            return placement_strategy;
        }

        /**
         * @return the invertedIndexingEnabled
         */
        public boolean isInvertedIndexingEnabled()
        {
            return invertedIndexingEnabled;
        }

        /**
         * @param invertedIndexingEnabled
         *            the invertedIndexingEnabled to set
         */
        public void setInvertedIndexingEnabled(boolean invertedIndexingEnabled)
        {
            this.invertedIndexingEnabled = invertedIndexingEnabled;
        }

        /**
         * @param placement_strategy
         *            the placement_strategy to set
         */
        public void setPlacement_strategy(String placement_strategy)
        {
            if (placement_strategy != null)
            {
                if (CassandraValidationClassMapper.getReplicationStrategies().contains(placement_strategy))
                {
                    this.placement_strategy = placement_strategy;
                }
                else
                {
                    this.placement_strategy = SimpleStrategy.class.getName();
                    log.warn("Give a valid replica placement strategy," + placement_strategy
                            + "is not a valid replica placement strategy");
                }
            }
        }

        /**
         * @return the dataCenterToNode
         */
        public Map<String, String> getDataCenters()
        {
            if (dataCentersInfo == null)
            {
                dataCentersInfo = new HashMap<String, String>();
            }
            return dataCentersInfo;
        }

        public void addDataCenter(String dataCenters)
        {
            if (dataCenters != null)
            {
                StringTokenizer stk = new StringTokenizer(dataCenters, ",");
                while (stk.hasMoreTokens())
                {
                    String dCName;
                    String noOfNode;
                    String[] token = { "dCName", "noOfNode" };
                    Map<String, String> dataCeneter = new HashMap<String, String>();
                    StringTokenizer tokenizer = new StringTokenizer(stk.nextToken(), ":");

                    int count = 0;
                    while (tokenizer.hasMoreTokens())
                    {
                        dataCeneter.put(token[count++], tokenizer.nextToken());
                    }
                    dCName = dataCeneter.get(token[0]);
                    noOfNode = dataCeneter.get(token[1]);
                    if (dCName != null && noOfNode != null)
                    {
                        getDataCenters().put(dCName, noOfNode);
                    }
                    else
                    {
                        // TODO
                        return;
                    }
                }
            }
        }

        public boolean isCounterColumn(String cfName)
        {
            return getColumnFamilyProperties().containsKey(cfName)
                    && getColumnFamilyProperties().get(cfName).getDefault_validation_class()
                            .equalsIgnoreCase(CounterColumnType.class.getSimpleName()) ? true : false;
        }
    }
}
