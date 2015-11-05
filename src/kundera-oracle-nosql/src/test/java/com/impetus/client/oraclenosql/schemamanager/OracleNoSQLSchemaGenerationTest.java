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
package com.impetus.client.oraclenosql.schemamanager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;

/**
 * The Class OracleNoSQLSchemaGenerationTest.
 * 
 * @author devender.yadav
 * 
 */
public class OracleNoSQLSchemaGenerationTest
{

    /** The Constant _PU. */
    private static final String _PU = "oracleNosqlSchemaGeneration";

    /** The Constant STORE_NAME. */
    private static final String STORE_NAME = "OracleNoSqlTests";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The kv store. */
    private static KVStore kvStore;

    /** The table api. */
    private static TableAPI tableAPI;

    /** The property map. */
    private Map<String, String> propertyMap = new HashMap<String, String>();

    /**
     * Sets the upbefore class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpbeforeClass() throws Exception
    {
        kvStore = KVStoreFactory.getStore(new KVStoreConfig(STORE_NAME, "localhost:5000"));
        tableAPI = kvStore.getTableAPI();
        tableAPI.executeSync("DROP TABLE IF EXISTS ONS_USER");
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
        tableAPI.executeSync("DROP TABLE IF EXISTS ONS_USER");
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        emf.close();
        kvStore.close();
    }

    /**
     * Test create.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testCreate() throws Exception
    {
        Table table = tableAPI.getTable("ONS_USER");
        Assert.assertNull(table);

        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);

        table = tableAPI.getTable("ONS_USER");
        Assert.assertNotNull(table);
        List<String> list = table.getFields();
        Assert.assertTrue(!list.isEmpty());
        Assert.assertNotNull(table.getIndexes());
    }

    /**
     * Test create drop.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testCreateDrop() throws Exception
    {
        Table table = tableAPI.getTable("ONS_USER");
        Assert.assertNull(table);

        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");
        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);

        table = tableAPI.getTable("ONS_USER");
        Assert.assertNotNull(table);
        List<String> list = table.getFields();
        Assert.assertTrue(!list.isEmpty());
        Assert.assertNotNull(table.getIndexes());
    }

    /**
     * Test update.
     */
    @Test
    public void testUpdate()
    {
        Table table = tableAPI.getTable("ONS_USER");
        Assert.assertNull(table);

        tableAPI.executeSync("CREATE TABLE ONS_USER(userId string, name string, age string, PRIMARY KEY(userId))");
        table = tableAPI.getTable("ONS_USER");
        Assert.assertNotNull(table);
        List<String> list = table.getFields();

        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "update");
        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
        table = tableAPI.getTable("ONS_USER");
        Assert.assertNotNull(table);
        List<String> updatedList = table.getFields();
        Assert.assertNotSame(list.size(), updatedList.size());
        Assert.assertNotNull(table.getIndexes());
    }

    /**
     * Test validate.
     */
    @Test
    public void testValidate()
    {
        Table table = tableAPI.getTable("ONS_USER");
        Assert.assertNull(table);

        try
        {
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "validate");
            emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
            Assert.fail("SchemaGenerationException must be thrown as there is no entity with name ONS_USER");
        }
        catch (Exception e)
        {
            Assert.assertNotNull(e);
        }
    }
}
