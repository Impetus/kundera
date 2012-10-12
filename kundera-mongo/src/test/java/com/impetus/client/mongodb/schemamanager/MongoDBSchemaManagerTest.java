/**
 * 
 */
package com.impetus.client.mongodb.schemamanager;

import java.lang.reflect.Field;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.mongodb.DB;
import com.mongodb.DBCollection;
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
    }

    /**
     * Test method for
     * {@link com.impetus.client.mongodb.schemamanager.MongoDBSchemaManager#create(java.util.List)}
     * .
     */
    @Test
    public void testCreate()
    {
        DBCollection collection = db.getCollection("MongoDBEntitySimple");
        Assert.assertTrue(collection.isCapped());
        Assert.assertEquals(ReadPreference.PRIMARY, collection.getReadPreference());
        Assert.assertNotNull(collection.getIndexInfo());
        Assert.assertEquals(2, collection.getIndexInfo().size());
    }

    /**
     * Test method for
     * {@link com.impetus.client.mongodb.schemamanager.MongoDBSchemaManager#dropSchema()}
     * .
     */
    // @Test
    public void testDropSchema()
    {

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
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (NoSuchFieldException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (IllegalArgumentException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (IllegalAccessException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
