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
package com.impetus.client.hbase.schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.SchemaGenerationException;

/**
 * The Class HBaseSchemaGenerationTest.
 * 
 * @author Pragalbh Garg
 */
public class HBaseSchemaGenerationTest
{

    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "schemaTest";

    /** The Constant SCHEMA. */
    private static final String SCHEMA = "HBaseNew";

    /** The TABL e_1. */
    private String TABLE_1 = HBaseUtils.getHTableName(SCHEMA, "USER_HBASE");

    /** The TABL e_2. */
    private String TABLE_2 = HBaseUtils.getHTableName(SCHEMA, "PRODUCT_HBASE");

    /** The admin. */
    private static HBaseAdmin admin;

    /** The property map. */
    private Map propertyMap = new HashMap();

    /** The connection. */
    private static Connection connection;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        connection = ConnectionFactory.createConnection();
        admin = (HBaseAdmin) connection.getAdmin();
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
        admin.close();
        connection.close();
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

    /**
     * Test operations.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @Test
    public void testOperations() throws IOException
    {
        testCreate();
        testUpdate();
        testValidate();
        testCreateDrop();
    }

    /**
     * Test create.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void testCreate() throws IOException
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        assertSchema();
    }

    /**
     * Test update.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void testUpdate() throws IOException
    {

        init();

        admin.disableTable(TABLE_2);
        admin.deleteTable(TABLE_2);
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "update");

        assertSchema();

        postAssert();
    }

    /**
     * Test validate.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void testValidate() throws IOException
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "validate");
        assertSchema();

        assertInValidate();

    }

    /**
     * Assert in validate.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void assertInValidate() throws IOException
    {
        admin.disableTable(TABLE_2);
        admin.deleteTable(TABLE_2);
        try
        {
            EntityManagerFactory emf = Persistence.createEntityManagerFactory(HBASE_PU, propertyMap);
            EntityManager em = emf.createEntityManager();
            Assert.assertTrue(false);
            em.close();
            emf.close();
        }
        catch (SchemaGenerationException sge)
        {
            Assert.assertTrue(true);
        }

    }

    /**
     * Test create drop.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void testCreateDrop() throws IOException
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");
        assertSchema();

        assertDrop();

    }

    /**
     * Assert drop.
     */
    private void assertDrop()
    {
        try
        {
            for (NamespaceDescriptor ns : admin.listNamespaceDescriptors())
            {
                if (ns.getName().equals(SCHEMA))
                {
                    Assert.assertTrue(false);
                    break;
                }
            }
        }
        catch (IOException ioex)
        {
            throw new SchemaGenerationException(ioex, "Hbase");
        }

    }

    /**
     * Assert schema.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void assertSchema() throws IOException
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(HBASE_PU, propertyMap);
        EntityManager em = emf.createEntityManager();

        try
        {
            admin.getNamespaceDescriptor(SCHEMA);
            Assert.assertTrue(true);
        }
        catch (NamespaceNotFoundException e)
        {
            Assert.assertTrue(false);
        }

        Assert.assertTrue(admin.isTableAvailable(TABLE_1));
        Assert.assertTrue(admin.isTableAvailable(TABLE_2));

        HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(TABLE_1));
        Assert.assertTrue(descriptor.hasFamily("USER_HBASE".getBytes()));

        descriptor = admin.getTableDescriptor(TableName.valueOf(TABLE_2));
        Assert.assertTrue(descriptor.hasFamily("PRODUCT_HBASE".getBytes()));

        em.close();
        emf.close();
    }

    /**
     * Inits the.
     */
    private void init()
    {
        propertyMap.remove(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(HBASE_PU, propertyMap);
        EntityManager em = emf.createEntityManager();
        UserHBase user = new UserHBase();
        user.setUserId("1");
        user.setUserName("personHbase");
        user.setPhoneNo(88888);
        em.persist(user);
        em.close();
        emf.close();

    }

    /**
     * Post assert.
     */
    private void postAssert()
    {
        propertyMap.remove(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(HBASE_PU, propertyMap);
        EntityManager em = emf.createEntityManager();
        try
        {
            UserHBase user = em.find(UserHBase.class, "1");
            if (user != null)
            {
                Assert.assertTrue(true);
            }
            else
            {
                Assert.assertTrue(false);
            }
        }
        catch (Exception e)
        {
            Assert.assertTrue(false);
        }

        em.close();
        emf.close();

    }
}