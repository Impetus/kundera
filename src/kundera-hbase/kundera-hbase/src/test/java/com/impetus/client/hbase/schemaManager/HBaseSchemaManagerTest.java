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
package com.impetus.client.hbase.schemaManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.PersistenceProperties;

/**
 * HbaseSchemaManagerTest class test the auto creation schema property in hbase
 * data store.
 * 
 * @author Kuldeep.Kumar
 * 
 */
public class HBaseSchemaManagerTest
{
    private final boolean useLucene = true;

    /** The admin. */
    private static HBaseAdmin admin;

    private HBaseCli cli;

    private String persistenceUnit = "hbase";

    /** The Constant logger. */
    private static final Logger logger = LoggerFactory.getLogger(HBaseSchemaManagerTest.class);

    private static final String TABLE = "KunderaHbaseExamples";

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        cli = new HBaseCli();
        logger.info("starting server");
        cli.startCluster();
        if (admin == null)
        {
            admin = cli.utility.getHBaseAdmin();
        }
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        cli.stopCluster();
    }

    /**
     * Test schema operation.
     */
    @Test
    public void testSchemaOperation()
    {
        try
        {
            Map propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
            EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
            Assert.assertTrue(admin.isTableAvailable(TABLE));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntitySimple"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntitySuper"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntityAddressUni1To1"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntityAddressUniMTo1"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntityAddressUni1ToM"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntityPersonUni1ToM"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntityPersonUni1To1"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntityPersonUniMto1"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntityAddressUni1To1PK"));
            Assert.assertFalse(admin.isTableAvailable("HbaseEntityPersonUni1To1PK"));
        }
        catch (IOException e)
        {
            Assert.fail("Failed, Caused by:" + e.getMessage());
        }
        catch (Exception e)
        {
            Assert.fail("Failed, Caused by:" + e.getMessage());
        }
    }
}
