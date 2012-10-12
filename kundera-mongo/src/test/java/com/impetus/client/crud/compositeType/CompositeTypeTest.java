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
package com.impetus.client.crud.compositeType;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.mongodb.DB;

/**
 * @author vivek.mishra
 * 
 */
public class CompositeTypeTest
{

    private EntityManagerFactory emf;

    private static final Log logger = LogFactory.getLog(CompositeTypeTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("mongoTest");
    }

    
    /**
     * CRUD over Compound primary Key.
     */
    @Test
    public void onCRUD()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CompoundKey key = new CompoundKey("mevivs", 1, timeLineId);
        PrimeUser user = new PrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(new Date());
        em.persist(user);

        em.clear(); // optional,just to clear persistence cache.

        PrimeUser result = em.find(PrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("my first tweet", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        em.clear();// optional,just to clear persistence cache.

        user.setTweetBody("After merge");
        em.merge(user);

        em.clear();// optional,just to clear persistence cache.

        result = em.find(PrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("After merge", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        em.remove(result);

        em.clear();// optional,just to clear persistence cache.

        result = em.find(PrimeUser.class, key);
        Assert.assertNull(result);
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
     * 
     */
    private void truncateMongo()
    {
        EntityManager em = emf.createEntityManager();

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        MongoDBClient client = (MongoDBClient) clients.get("mongoTest");
        if (client != null)
        {
            try
            {
                Field db = client.getClass().getDeclaredField("mongoDb");
                if (!db.isAccessible())
                {
                    db.setAccessible(true);
                }
                DB mongoDB = (DB) db.get(client);
                mongoDB.dropDatabase();
            }
            catch (SecurityException e)
            {
                logger.error(e);
            }

            catch (NoSuchFieldException e)
            {
                logger.error(e);
            }
            catch (IllegalArgumentException e)
            {
                logger.error(e);
            }
            catch (IllegalAccessException e)
            {
                logger.error(e);
            }
        }

    }
}
