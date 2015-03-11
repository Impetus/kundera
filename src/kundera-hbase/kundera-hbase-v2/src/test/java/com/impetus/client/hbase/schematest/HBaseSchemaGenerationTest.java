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
package com.impetus.client.hbase.schematest;

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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.client.hbase.testingutil.HBaseTestingUtils;
import com.impetus.client.hbase.utils.HBaseUtils;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.configure.schema.SchemaGenerationException;

/**
 * @author Pragalbh Garg
 *
 */
public class HBaseSchemaGenerationTest
{

    private static final String SCHEMA = "HBaseNew";

    private String TABLE_1 = HBaseUtils.getHTableName(SCHEMA, "USER_HBASE");

    private String TABLE_2 = HBaseUtils.getHTableName(SCHEMA, "PRODUCT_HBASE");

    private static HBaseAdmin admin;

    private static HBaseCli cli;

    private String persistenceUnit = "schemaTest";

    private Map propertyMap = new HashMap();

//    private static Connection connection;
    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        cli = new HBaseCli();
        cli.startCluster();
        Connection connection = ConnectionFactory.createConnection();
        admin = (HBaseAdmin) connection.getAdmin();
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
        HBaseTestingUtils.dropSchema(SCHEMA);
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
    public void testOperations() throws IOException
    {
        testCreate();
        testUpdate();
        testValidate();
        testCreateDrop();
    }

    public void testCreate() throws IOException
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        assertSchema();
    }

    public void testUpdate() throws IOException
    {
        
        init();
        
        admin.disableTable(TABLE_2);
        admin.deleteTable(TABLE_2);
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "update");
        
        assertSchema();
        
        postAssert();
    }

    private void testValidate() throws IOException
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "validate");
        assertSchema();
    
        assertInValidate();
    
    }

    private void assertInValidate() throws IOException
    {
        admin.disableTable(TABLE_2);
        admin.deleteTable(TABLE_2);
        try
        {
            EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
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

    public void testCreateDrop() throws IOException
    {
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");
        assertSchema();
        
        assertDrop();
        
    }

    private void assertDrop()
    {
        try
        {
            for(NamespaceDescriptor ns : admin.listNamespaceDescriptors()){
                if(ns.getName().equals(SCHEMA)){
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

    private void assertSchema() throws IOException{
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
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

    private void init()
    {
        propertyMap.remove(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        EntityManager em = emf.createEntityManager();
        UserHBase user = new UserHBase();
        user.setUserId("1");
        user.setUserName("personHbase");
        user.setPhoneNo(88888);
        em.persist(user);
        em.close();
        emf.close();
        
    }
    
    private void postAssert()
    {
        propertyMap.remove(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        EntityManager em = emf.createEntityManager();
        try{
            UserHBase user = em.find(UserHBase.class, "1");
            if(user!=null){
            Assert.assertTrue(true);
            }else{
                Assert.assertTrue(false); 
            }
        }catch(Exception e){
            Assert.assertTrue(false);
        }
        
        em.close();
        emf.close();
        
    }
}