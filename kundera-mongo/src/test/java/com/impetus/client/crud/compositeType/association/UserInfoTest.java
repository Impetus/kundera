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
package com.impetus.client.crud.compositeType.association;


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

import com.impetus.client.crud.compositeType.CompositeTypeTest;
import com.impetus.client.crud.compositeType.CompoundKey;
import com.impetus.client.crud.compositeType.PrimeUser;
import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.mongodb.DB;

/**
 * @author vivek.mishra
 *
 */
public class UserInfoTest
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

    @Test
    public void onCRUD()
    {
        EntityManager em = emf.createEntityManager();
        
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CompoundKey key = new CompoundKey("mevivs", 1, timeLineId.toString());
        PrimeUser timeLine = new PrimeUser(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(new Date());
        
        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31, timeLine);
        em.persist(userInfo);
        em.clear();
        
        UserInfo result = em.find(UserInfo.class, "mevivs_info");
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate, result.getTimeLine().getTweetDate());
        Assert.assertEquals(timeLineId.toString(), result.getTimeLine().getKey().getTimeLineId());
        Assert.assertEquals("Vivek", userInfo.getFirstName());
        Assert.assertEquals(31, userInfo.getAge());
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
