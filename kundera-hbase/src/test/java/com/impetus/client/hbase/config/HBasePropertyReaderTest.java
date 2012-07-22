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

import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

public class HBasePropertyReaderTest
{
    private static Log log = LogFactory.getLog(HBasePropertyReaderTest.class);

    private String port = "2181";

    private String host = "localhost";

    /**
     * persistence unit pu.
     */
    private String pu = "hbaseTest";

    private HBaseColumnFamilyProperties prop = null;

    @Before
    public void setUp() throws Exception
    {
//        Persistence.createEntityManagerFactory(pu);
        new PersistenceUnitConfiguration(pu).configure();
        new ClientFactoryConfiguraton(pu).configure();
    }

    @After
    public void tearDown() throws Exception
    {
        
    }

    @Test
    public void testRead() throws IOException
    {
        testReadProperty();
    }

    /**
     * @throws IOException
     */
    private void testReadProperty() throws IOException
    {
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        Properties properties = new Properties();
        String property = puMetadata.getProperty(PersistenceProperties.KUNDERA_CLIENT_PROPERTY);
        InputStream inStream = null;
        if (property != null)
        {
            inStream = ClassLoader.getSystemResourceAsStream(property);
        }
        HBaseColumnFamilyProperties familyProperties = new HBaseColumnFamilyProperties();
        if (inStream != null)
        {
            properties.load(inStream);
            port = properties.getProperty(Constants.ZOOKEEPER_PORT);
            host = properties.getProperty(Constants.ZOOKEEPER_HOST);
            String cfDefs = properties.getProperty(Constants.CF_DEFS);
            if (cfDefs != null)
            {

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

                prop = HBasePropertyReader.hsmd.getColumnFamilyProperties().get(tokenNames[0]);
            }
            if (prop != null)
            {
                Assert.assertEquals(familyProperties.getMaxVersion(), prop.getMaxVersion());
                Assert.assertEquals(familyProperties.getMinVersion(), prop.getMinVersion());
                Assert.assertEquals(familyProperties.getTtl(), prop.getTtl());
                Assert.assertEquals(familyProperties.getAlgorithm(), prop.getAlgorithm());
            }

        }
        else
        {
            // Assertion on default property set in persistence.xml
            Assert.assertEquals(puMetadata.getProperty(PersistenceProperties.KUNDERA_NODES), HBasePropertyReader.hsmd.getZookeeperHost());
            Assert.assertEquals(puMetadata.getProperty(PersistenceProperties.KUNDERA_PORT), HBasePropertyReader.hsmd.getZookeeperPort());
        }
           // not same as reading default properties
        Assert.assertNotSame(port, HBasePropertyReader.hsmd.getZookeeperPort());
        Assert.assertNotSame(host, HBasePropertyReader.hsmd.getZookeeperHost());
    }
        
}
