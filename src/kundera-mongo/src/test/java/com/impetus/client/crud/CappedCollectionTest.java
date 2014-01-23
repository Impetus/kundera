/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.crud;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.mongodb.MongoDBConstants;
import com.impetus.kundera.client.Client;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Test case for Capped Collections
 * 
 * @author amresh.singh
 */
public class CappedCollectionTest
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
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
        em.getDelegate();
        getDB();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        truncateMongo();
        emf.close();
    }

    @Test
    public void testCappedCollection()
    {
        // Validate schema
        DBCollection collection = db.getCollection("MongoDBCappedEntity");
        Assert.assertTrue(collection.isCapped());
        CommandResult stats = collection.getStats();

        Object maxObj = stats.get(MongoDBConstants.MAX);
        Assert.assertNotNull(maxObj);
        int max = Integer.parseInt(maxObj.toString());
        Assert.assertEquals(10, max);

        /*
         * Object size = stats.get(MongoDBConstants.SIZE);
         * Assert.assertNotNull(size); Assert.assertEquals(10240,
         * Integer.parseInt(size.toString()));
         */

        // Index should not have been created
        List<DBObject> indexInfo = collection.getIndexInfo();
        Assert.assertTrue(indexInfo.isEmpty() || indexInfo.size() == 1);

        // Insert "Max" records successfully
        for (int i = 1; i <= max; i++)
        {
            MongoDBCappedEntity entity = new MongoDBCappedEntity();
            entity.setPersonId(i + "");
            entity.setPersonName("Person_Name_" + i);
            entity.setAge((short) i);

            em.persist(entity);
        }

        // Insert one more record and check whether it replaces first one
        MongoDBCappedEntity entity = new MongoDBCappedEntity();
        entity.setPersonId((max + 1) + "");
        entity.setPersonName("Person_Name_" + (max + 1));
        entity.setAge((short) (max + 1));
        em.persist(entity);

        em.clear();
        MongoDBCappedEntity firstRecord = em.find(MongoDBCappedEntity.class, "1");
        Assert.assertNull(firstRecord);

        MongoDBCappedEntity lastRecord = em.find(MongoDBCappedEntity.class, "" + (max + 1));
        Assert.assertNotNull(lastRecord);

        // Deleting documents within a capped collection should simply ignore,
        // as is done by native API
        em.remove(lastRecord);
        em.clear();
        lastRecord = em.find(MongoDBCappedEntity.class, "" + (max + 1));
        Assert.assertNotNull(lastRecord);

    }

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
                // TODO Auto-generated catch block
                
            }
            catch (NoSuchFieldException e)
            {
                // TODO Auto-generated catch block
                
            }
            catch (IllegalArgumentException e)
            {
                // TODO Auto-generated catch block
                
            }
            catch (IllegalAccessException e)
            {
                // TODO Auto-generated catch block
                
            }
        }
    }

}
