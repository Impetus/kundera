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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.HBaseClientFactory;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.configure.schema.SchemaGenerationException;

/**
 * @author Kuldeep.Kumar
 * 
 */
public class HBaseSchemaOperationTest
{
    private static final String HBASE_ENTITY_SIMPLE = "HbaseEntitySimple";

    private static final String TABLE = "KunderaHbaseTests";

    private static HBaseAdmin admin;

    private static HBaseCli cli;

    private String persistenceUnit = "HBaseSchemaOperationTest";

    private Map propertyMap = new HashMap();

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        cli = new HBaseCli();
        cli.startCluster();
        if (admin == null)
        {
            admin = HBaseCli.utility.getHBaseAdmin();
        }
    }

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
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        cli.dropTable(TABLE);
        HBaseCli.stopCluster();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testCreate() throws IOException
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        Assert.assertTrue(admin.isTableAvailable(TABLE));

        HTableDescriptor descriptor = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor.getFamilies());
        Assert.assertEquals(1, descriptor.getFamilies().size());
        for (HColumnDescriptor columnDescriptor : descriptor.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertEquals(HBASE_ENTITY_SIMPLE, columnDescriptor.getNameAsString());
        }

        admin.disableTable(TABLE);
        admin.deleteTable(TABLE);
        Assert.assertFalse(admin.isTableAvailable(TABLE));
    }

    @Test
    public void testCreatedrop() throws IOException
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        // schemaManager = new
        // HBaseSchemaManager(HBaseClientFactory.class.getName(), null);
        // schemaManager.exportSchema();
        Assert.assertTrue(admin.isTableAvailable(TABLE));

        HTableDescriptor descriptor = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor.getFamilies());
        Assert.assertEquals(1, descriptor.getFamilies().size());
        for (HColumnDescriptor columnDescriptor : descriptor.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertEquals(HBASE_ENTITY_SIMPLE, columnDescriptor.getNameAsString());
        }

        HBaseClientFactory clientFactory = (HBaseClientFactory) ClientResolver.getClientFactory(persistenceUnit);
        clientFactory.getSchemaManager(null).dropSchema();
        Assert.assertTrue(admin.isTableAvailable(TABLE));
        Assert.assertNull(admin.getTableDescriptor(TABLE.getBytes()).getFamily(HBASE_ENTITY_SIMPLE.getBytes()));
    }

    @Test
    public void testUpdate() throws IOException
    {
        HTableDescriptor descriptor1 = new HTableDescriptor(TABLE);
        HColumnDescriptor columnDescriptor1 = new HColumnDescriptor("PERSON_NAME");
        descriptor1.addFamily(columnDescriptor1);
        if (admin.isTableAvailable(TABLE))
        {
            admin.disableTable(TABLE);
            admin.deleteTable(TABLE);
        }
        admin.createTable(descriptor1);
        Assert.assertTrue(admin.isTableAvailable(TABLE));
        HTableDescriptor descriptor2 = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor2.getFamilies());
        Assert.assertEquals(1, descriptor2.getFamilies().size());
        for (HColumnDescriptor columnDescriptor : descriptor2.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertEquals("PERSON_NAME", columnDescriptor.getNameAsString());
        }

        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "update");
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        // schemaManager = new
        // HBaseSchemaManager(HBaseClientFactory.class.getName(), null);
        // schemaManager.exportSchema();

        Assert.assertTrue(admin.isTableAvailable(TABLE));

        HTableDescriptor descriptor = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor.getFamilies());
        Assert.assertEquals(2, descriptor.getFamilies().size());
        List<String> columns = new ArrayList<String>();
        columns.add(HBASE_ENTITY_SIMPLE);
        columns.add("PERSON_NAME");
        for (HColumnDescriptor columnDescriptor : descriptor.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertTrue(columns.contains(columnDescriptor.getNameAsString()));
        }

        if (!admin.isTableDisabled(TABLE))
        {
            admin.disableTable(TABLE);
        }

        admin.deleteTable(TABLE);
        Assert.assertFalse(admin.isTableAvailable(TABLE));
    }

    @Test
    public void testValidate() throws IOException
    {
        HTableDescriptor descriptor1 = new HTableDescriptor(TABLE);
        HColumnDescriptor columnDescriptor1 = new HColumnDescriptor(HBASE_ENTITY_SIMPLE);
        descriptor1.addFamily(columnDescriptor1);
        if (admin.isTableAvailable(TABLE))
        {
            admin.disableTable(TABLE);
            admin.deleteTable(TABLE);
        }
        admin.createTable(descriptor1);

        Assert.assertTrue(admin.isTableAvailable(TABLE));
        HTableDescriptor descriptor2 = admin.getTableDescriptor(TABLE.getBytes());
        Assert.assertNotNull(descriptor2.getFamilies());
        Assert.assertEquals(1, descriptor2.getFamilies().size());
        List<String> columns = new ArrayList<String>();
        columns.add(HBASE_ENTITY_SIMPLE);
        for (HColumnDescriptor columnDescriptor : descriptor2.getFamilies())
        {
            Assert.assertNotNull(columnDescriptor);
            Assert.assertNotNull(columnDescriptor.getNameAsString());
            Assert.assertTrue(columns.contains(columnDescriptor.getNameAsString()));
        }

        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "validate");
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        // schemaManager = new
        // HBaseSchemaManager(HBaseClientFactory.class.getName(), null);
        // schemaManager.exportSchema();
        if (!admin.isTableDisabled(TABLE))
        {
            admin.disableTable(TABLE);
        }
        admin.deleteTable(TABLE);
        Assert.assertFalse(admin.isTableAvailable(TABLE));
    }

    @Test
    public void testValidateInValid() throws IOException
    {
        try
        {
            HTableDescriptor descriptor1 = new HTableDescriptor(TABLE);
            HColumnDescriptor columnDescriptor1 = new HColumnDescriptor(HBASE_ENTITY_SIMPLE);
            descriptor1.addFamily(columnDescriptor1);
            if (admin.isTableAvailable(TABLE))
            {
                admin.disableTable(TABLE);
                admin.deleteTable(TABLE);
            }
            admin.createTable(descriptor1);
            Assert.assertTrue(admin.isTableAvailable(TABLE));
            HTableDescriptor descriptor2 = admin.getTableDescriptor(TABLE.getBytes());
            Assert.assertNotNull(descriptor2.getFamilies());
            Assert.assertEquals(1, descriptor2.getFamilies().size());
            for (HColumnDescriptor columnDescriptor : descriptor2.getFamilies())
            {
                Assert.assertNotNull(columnDescriptor);
                Assert.assertNotNull(columnDescriptor.getNameAsString());
                Assert.assertEquals(HBASE_ENTITY_SIMPLE, columnDescriptor.getNameAsString());
            }

            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "validate");
            EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
            // schemaManager = new
            // HBaseSchemaManager(HBaseClientFactory.class.getName(), null);
            // schemaManager.exportSchema();
        }
        catch (SchemaGenerationException sgex)
        {
            List<String> errors = new ArrayList<String>();
            errors.add("column " + "AGE" + " does not exist in table " + TABLE + "");
            errors.add("column " + "PERSON_NAME" + " does not exist in table " + TABLE + "");
            Assert.assertTrue(errors.contains(sgex.getMessage()));

        }
    }
}
