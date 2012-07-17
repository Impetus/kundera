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

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.config.CassandraPropertyReader.CassandraSchemaMetadata;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * @author impadmin
 * 
 */
public class CassandraPropertyReaderTest
{
    private Map<String, CassandraColumnFamilyProperties> familyToProperties = new HashMap<String, CassandraColumnFamilyProperties>();

    private Map<String, String> dataCenterToNode = new HashMap<String, String>();

    private String replication;

    private String strategy;

    /** log instance */
    private org.apache.commons.logging.Log log = LogFactory.getLog(CassandraPropertyReaderTest.class);

    private CassandraSchemaMetadata metadata;

    private String pu = "CassandraCounterTest";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for
     * {@link com.impetus.client.cassandra.config.CassandraPropertyReader#readProperty()}
     * .
     * 
     * @throws IOException
     */
    @Test
    public void testReadProperty() throws IOException
    {
        log.info("running CassandraPropertyReaderTest");
        // reader = new CassandraPropertyReader();
        // reader.read("CassandraCounterTest");
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(pu);
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        // metadata = CassandraPropertyReader.csmd;
        Properties properties = new Properties();
        InputStream inStream = ClassLoader.getSystemResourceAsStream(puMetadata
                .getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY));

        if (inStream != null)
        {
            properties.load(inStream);
            replication = properties.getProperty(Constants.REPLICATION_FACTOR);
            strategy = properties.getProperty(Constants.PLACEMENT_STRATEGY);
            String dataCenters = properties.getProperty(Constants.DATA_CENTERS);
            if (dataCenters != null)
            {
                StringTokenizer stk = new StringTokenizer(dataCenters, ",");
                while (stk.hasMoreTokens())
                {
                    StringTokenizer tokenizer = new StringTokenizer(stk.nextToken(), ":");
                    if (tokenizer.countTokens() == 2)
                    {
                        dataCenterToNode.put(tokenizer.nextToken(), tokenizer.nextToken());
                    }
                }
            }

            String cf_defs = properties.getProperty(Constants.CF_DEFS);
            if (cf_defs != null)
            {
                StringTokenizer stk = new StringTokenizer(cf_defs, ",");
                while (stk.hasMoreTokens())
                {
                    CassandraColumnFamilyProperties familyProperties = new CassandraColumnFamilyProperties();
                    StringTokenizer tokenizer = new StringTokenizer(stk.nextToken(), "|");
                    if (tokenizer.countTokens() != 0 && tokenizer.countTokens() >= 2)
                    {
                        String columnFamilyName = tokenizer.nextToken();
                        String defaultValidationClass = tokenizer.nextToken();
                        familyProperties.setDefault_validation_class(defaultValidationClass);

                        if (tokenizer.countTokens() != 0)
                        {
                            String comparator = tokenizer.nextToken();

                            familyProperties.setComparator(comparator);

                        }
                        familyToProperties.put(columnFamilyName, familyProperties);
                    }
                }
            }

            metadata = CassandraPropertyReader.csmd;
            if (replication != null)
            {
                Assert.assertNotNull(replication);
                Assert.assertNotNull(metadata.getReplication_factor());

            }
            else
            {
                Assert.assertNull(replication);
                Assert.assertNull(metadata.getReplication_factor());
            }
            Assert.assertEquals(replication, metadata.getReplication_factor());

            if (strategy != null)
            {
                Assert.assertNotNull(strategy);
                Assert.assertNotNull(metadata.getPlacement_strategy());

            }
            else
            {
                Assert.assertNull(strategy);
                Assert.assertNull(metadata.getPlacement_strategy());
            }
            Assert.assertEquals(strategy, metadata.getPlacement_strategy());

            if (!familyToProperties.isEmpty())
            {
                Assert.assertNotNull(familyToProperties);
                Assert.assertNotNull(metadata.getColumnFamilyProperties());
                Assert.assertFalse(metadata.getColumnFamilyProperties().isEmpty());
            }
            else
            {
                Assert.assertTrue(metadata.getColumnFamilyProperties().isEmpty());
            }
            Assert.assertEquals(familyToProperties.size(), metadata.getColumnFamilyProperties().size());

            if (!dataCenterToNode.isEmpty())
            {
                Assert.assertNotNull(dataCenterToNode);
                Assert.assertNotNull(metadata.getDataCenters());
                Assert.assertFalse(metadata.getDataCenters().isEmpty());
            }
            else
            {
                Assert.assertTrue(metadata.getDataCenters().isEmpty());
            }
            Assert.assertEquals(dataCenterToNode.size(), metadata.getDataCenters().size());
        }
        else
        {
            Assert.assertEquals("1", metadata.getReplication_factor());
            Assert.assertEquals(SimpleStrategy.class.getName(), metadata.getPlacement_strategy());
            Assert.assertEquals(0, metadata.getDataCenters().size());
            Assert.assertEquals(0, metadata.getColumnFamilyProperties().size());
        }
    }
}
