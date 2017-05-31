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
package com.impetus.client.mongodb.schemamanager;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.mongodb.MongoDBClientFactory;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientResolver;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MongoDBSchemaManagerTest
{
    private DB db;

    private String persistenceUnit = "mongoSchemaGenerationTest";

    private EntityManagerFactory emf;

    private EntityManager em;

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
        truncateMongo();
    }

    @Test
    public void testValidate()
    {

        try
        {
            Map props = new HashMap();
            props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "validate");
            emf = Persistence.createEntityManagerFactory(persistenceUnit, props);
            Assert.fail("Schema generation exception should have been thrown since schema doesn't exist");
        }
        catch (Exception e)
        {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void testUpdate()
    {

        Map props = new HashMap();
        props.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "update");

        emf = Persistence.createEntityManagerFactory(persistenceUnit, props);
        em = emf.createEntityManager();

        getDB();

        DBCollection collection = db.getCollection("MongoDBEntitySimple");
        Assert.assertEquals(ReadPreference.primary(), collection.getReadPreference());
        Assert.assertNotNull(collection.getIndexInfo());
        Assert.assertEquals(4, collection.getIndexInfo().size());
        int count = 0;
        for (DBObject dbObject : collection.getIndexInfo())
        {

            if (dbObject.get("name").equals("_id_"))
            {
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("_id", 1)));
                count++;
            }
            else if (dbObject.get("name").equals("PERSON_NAME_1"))
            {
                Assert.assertEquals(new Double(Integer.MIN_VALUE), dbObject.get("min"));
                Assert.assertEquals(new Double(Integer.MAX_VALUE), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("PERSON_NAME", 1)));
                count++;
            }
            else if (dbObject.get("name").equals("AGE_-1"))
            {
                Assert.assertEquals(new Double(100.0), dbObject.get("min"));
                Assert.assertEquals(new Double(500.0), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("AGE", -1)));
                count++;
            }
            else
            {
                Assert.assertEquals(new Double(-100), dbObject.get("min"));
                Assert.assertEquals(new Double(500), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("CURRENT_LOCATION", "2d")));
                count++;
            }
        }
        Assert.assertEquals(4, count);

        collection = db.getCollection("GFS_BOOK.files");
        Assert.assertEquals(ReadPreference.primary(), collection.getReadPreference());
        Assert.assertNotNull(collection.getIndexInfo());

        collection = db.getCollection("GFS_BOOK.chunks");
        Assert.assertEquals(ReadPreference.primary(), collection.getReadPreference());
        Assert.assertNotNull(collection.getIndexInfo());
    }

    /**
     * Test method for
     * {@link com.impetus.client.mongodb.schemamanager.MongoDBSchemaManager#create(java.util.List)}
     * .
     */
    @Test
    public void testCreate()
    {
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
        getDB();

        DBCollection collection = db.getCollection("MongoDBEntitySimple");
        Assert.assertEquals(ReadPreference.primary(), collection.getReadPreference());
        Assert.assertNotNull(collection.getIndexInfo());
        Assert.assertEquals(4, collection.getIndexInfo().size());
        int count = 0;
        for (DBObject dbObject : collection.getIndexInfo())
        {

            if (dbObject.get("name").equals("_id_"))
            {
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("_id", 1)));
                count++;
            }
            else if (dbObject.get("name").equals("PERSON_NAME_1"))
            {
                Assert.assertEquals(new Double(Integer.MIN_VALUE), dbObject.get("min"));
                Assert.assertEquals(new Double(Integer.MAX_VALUE), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("PERSON_NAME", 1)));
                count++;
            }
            else if (dbObject.get("name").equals("AGE_-1"))
            {
                Assert.assertEquals(new Double(100), dbObject.get("min"));
                Assert.assertEquals(new Double(500), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("AGE", -1)));
                count++;
            }
            else
            {
                Assert.assertEquals(new Double(-100), dbObject.get("min"));
                Assert.assertEquals(new Double(500), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("CURRENT_LOCATION", "2d")));
                count++;
            }
        }
        Assert.assertEquals(4, count);

        collection = db.getCollection("GFS_BOOK.files");
        Assert.assertEquals(ReadPreference.primary(), collection.getReadPreference());
        Assert.assertNotNull(collection.getIndexInfo());

        collection = db.getCollection("GFS_BOOK.chunks");
        Assert.assertEquals(ReadPreference.primary(), collection.getReadPreference());
        Assert.assertNotNull(collection.getIndexInfo());
    }

    @Test
    public void testCreateIndexesOnEmbedded()
    {
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
        getDB();

        DBCollection collection = db.getCollection("MongoDBEmbeddedEntity");
        Assert.assertEquals(ReadPreference.primary(), collection.getReadPreference());
        Assert.assertNotNull(collection.getIndexInfo());
        Assert.assertEquals(5, collection.getIndexInfo().size());
        int count = 0;
        for (DBObject dbObject : collection.getIndexInfo())
        {

            if (dbObject.get("name").equals("_id_"))
            {
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("_id", 1)));
                count++;
            }
            else if (dbObject.get("name").equals("PERSON_NAME_1"))
            {
                Assert.assertEquals(new Double(Integer.MIN_VALUE), dbObject.get("min"));
                Assert.assertEquals(new Double(Integer.MAX_VALUE), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("PERSON_NAME", 1)));
                count++;
            }
            else if (dbObject.get("name").equals("AGE_-1"))
            {
                Assert.assertEquals(new Double(100), dbObject.get("min"));
                Assert.assertEquals(new Double(500), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("AGE", -1)));
                count++;
            }
            else if (dbObject.get("name").equals("location.CURRENT_LOCATION_2d"))
            {
                Assert.assertEquals(new Double(-100), dbObject.get("min"));
                Assert.assertEquals(new Double(500), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("location.CURRENT_LOCATION", "2d")));
                count++;
            }
            else
            {
                Assert.assertEquals(new Double(100), dbObject.get("min"));
                Assert.assertEquals(new Double(400), dbObject.get("max"));
                Assert.assertTrue(dbObject.get("key").equals(new BasicDBObject("location.PREVIOUS_LOCATION", "2d")));
                count++;
            }
        }
        Assert.assertEquals(5, count);
    }

    /**
     * Test method for
     * {@link com.impetus.client.mongodb.schemamanager.MongoDBSchemaManager#dropSchema()}
     * .
     */
    @Test
    public void testDropSchema()
    {
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
        getDB();

        MongoDBClientFactory clientFactory = (MongoDBClientFactory) ClientResolver.getClientFactory(persistenceUnit);
        clientFactory.getSchemaManager(null).dropSchema();
        DBCollection collection = db.getCollection("MongoDBEntitySimple");
        Assert.assertTrue(collection.getIndexInfo().isEmpty());
        Assert.assertEquals(0, collection.getCount());
        collection = db.getCollection("GFS_BOOK.files");
        Assert.assertTrue(collection.getIndexInfo().isEmpty());
        Assert.assertEquals(0, collection.getCount());
        collection = db.getCollection("GFS_BOOK.chunks");
        Assert.assertTrue(collection.getIndexInfo().isEmpty());
        Assert.assertEquals(0, collection.getCount());
    }

    /**
     * 
     */
    private void truncateMongo()
    {

        if (db != null)
        {
            db.dropDatabase();
        }
    }

    /**
     * 
     */
    private void getDB()
    {
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        MongoDBClient client = (MongoDBClient) clients.get(persistenceUnit);
        if (client != null)
        {
            try
            {
                Field mongodb = client.getClass().getDeclaredField("mongoDb");
                if (!mongodb.isAccessible())
                {
                    mongodb.setAccessible(true);
                }
                db = (DB) mongodb.get(client);
            }

            catch (SecurityException e)
            {
                Assert.fail(e.getMessage());
            }
            catch (NoSuchFieldException e)
            {
                Assert.fail(e.getMessage());
            }
            catch (IllegalArgumentException e)
            {
                Assert.fail(e.getMessage());
            }
            catch (IllegalAccessException e)
            {
                Assert.fail(e.getMessage());
            }
        }
    }
}
