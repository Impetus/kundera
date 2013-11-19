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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.crud.compositeType.MongoCompositeTypeTest;
import com.impetus.client.crud.compositeType.MongoCompoundKey;
import com.impetus.client.crud.compositeType.MongoPrimeUser;
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

    private static final Logger logger = LoggerFactory.getLogger(MongoCompositeTypeTest.class);

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

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);
        MongoPrimeUser timeLine = new MongoPrimeUser(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(currentDate);

        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31, timeLine);
        em.persist(userInfo);
        em.clear();

        // Find
        UserInfo result = em.find(UserInfo.class, "mevivs_info");
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate, result.getTimeLine().get(0).getTweetDate());
        Assert.assertEquals(timeLineId, result.getTimeLine().get(0).getKey().getTimeLineId());
        Assert.assertEquals("Vivek", result.getFirstName());
        Assert.assertEquals(31, result.getAge());

        result.setFirstName("Kuldeep");
        result.setAge(23);

        em.merge(result);

        em.clear();

        // Find
        result = null;
        result = em.find(UserInfo.class, "mevivs_info");
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate, result.getTimeLine().get(0).getTweetDate());
        Assert.assertEquals(timeLineId, result.getTimeLine().get(0).getKey().getTimeLineId());
        Assert.assertEquals("Kuldeep", result.getFirstName());
        Assert.assertEquals(23, result.getAge());

        em.remove(result);

        em.clear();
        result = em.find(UserInfo.class, "mevivs_info");
        Assert.assertNull(result);

    }

    @Test
    public void onQuery()
    {
        EntityManager em = emf.createEntityManager();

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);
        MongoPrimeUser timeLine = new MongoPrimeUser(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(new Date());

        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31, timeLine);
        em.persist(userInfo);

        em.clear(); // optional,just to clear persistence cache.
        final String noClause = "Select u from UserInfo u";

        final String withClauseOnNoncomposite = "Select u from UserInfo u where u.age = ?1";

        // NOSQL Intelligence to teach that query is invalid because partition
        // key is not present?
        final String withAllCompositeColClause = "Select u from UserInfo u where u.userInfoId = :id";

        // query over 1 composite and 1 non-column

        // query with no clause.
        Query q = em.createQuery(noClause);
        List<UserInfo> results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(31, results.get(0).getAge());
        Assert.assertEquals("Vivek", results.get(0).getFirstName());
        Assert.assertEquals("Mishra", results.get(0).getLastName());
        Assert.assertEquals("mevivs_info", results.get(0).getUserInfoId());
        Assert.assertEquals(currentDate.getTime(), results.get(0).getTimeLine().get(0).getTweetDate().getTime());
        Assert.assertEquals(timeLineId, results.get(0).getTimeLine().get(0).getKey().getTimeLineId());

        // Query with composite key clause.
        q = em.createQuery(withClauseOnNoncomposite);
        q.setParameter(1, 31);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(31, results.get(0).getAge());
        Assert.assertEquals("Vivek", results.get(0).getFirstName());
        Assert.assertEquals("Mishra", results.get(0).getLastName());
        Assert.assertEquals("mevivs_info", results.get(0).getUserInfoId());
        Assert.assertEquals(currentDate, results.get(0).getTimeLine().get(0).getTweetDate());
        Assert.assertEquals(timeLineId, results.get(0).getTimeLine().get(0).getKey().getTimeLineId());

        // Query with composite key clause.
        q = em.createQuery(withAllCompositeColClause);
        q.setParameter("id", "mevivs_info");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(31, results.get(0).getAge());
        Assert.assertEquals("Vivek", results.get(0).getFirstName());
        Assert.assertEquals("Mishra", results.get(0).getLastName());
        Assert.assertEquals("mevivs_info", results.get(0).getUserInfoId());
        Assert.assertEquals(currentDate, results.get(0).getTimeLine().get(0).getTweetDate());
        Assert.assertEquals(timeLineId, results.get(0).getTimeLine().get(0).getKey().getTimeLineId());

        final String selectiveColumnTweetBodyWithAllCompositeColClause = "Select u.firstName from UserInfo u where u.userInfoId = :id";
        // Query with composite key clause.
        q = em.createQuery(selectiveColumnTweetBodyWithAllCompositeColClause);
        q.setParameter("id", "mevivs_info");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(0, results.get(0).getAge());
        Assert.assertEquals("Vivek", results.get(0).getFirstName());
        Assert.assertEquals(null, results.get(0).getLastName());
        Assert.assertEquals("mevivs_info", results.get(0).getUserInfoId());

        final String selectiveColumnTweetDateWithAllCompositeColClause = "Select u.lastName from UserInfo u where u.userInfoId = :id";
        // Query with composite key clause.
        q = em.createQuery(selectiveColumnTweetDateWithAllCompositeColClause);
        q.setParameter("id", "mevivs_info");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(0, results.get(0).getAge());
        Assert.assertEquals(null, results.get(0).getFirstName());
        Assert.assertEquals("Mishra", results.get(0).getLastName());
        Assert.assertEquals("mevivs_info", results.get(0).getUserInfoId());

        em.remove(userInfo);

        em.clear();// optional,just to clear persistence cache.
    }

    @Test
    public void onNamedQueryTest()
    {
        updateNamed();
        deleteNamed();

    }

    /**
     * @return
     */
    private void updateNamed()
    {
        EntityManager em = emf.createEntityManager();

        // Persist
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);
        MongoPrimeUser timeLine = new MongoPrimeUser(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(currentDate);

        UserInfo userInfo = new UserInfo("mevivs_info", "Vivek", "Mishra", 31, timeLine);
        em.persist(userInfo);

        em = emf.createEntityManager();

        String updateQuery = "Update UserInfo u SET u.firstName=Kuldeep where u.firstName= :beforeUpdate";
        Query q = em.createQuery(updateQuery);
        q.setParameter("beforeUpdate", "Vivek");
        q.executeUpdate();

        UserInfo result = em.find(UserInfo.class, "mevivs_info");
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate, result.getTimeLine().get(0).getTweetDate());
        Assert.assertEquals(timeLineId, result.getTimeLine().get(0).getKey().getTimeLineId());
        Assert.assertEquals("Kuldeep", result.getFirstName());
        Assert.assertEquals(31, result.getAge());
        em.close();
    }

    /**
     * 
     */
    private void deleteNamed()
    {
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);

        String deleteQuery = "Delete From UserInfo u where u.firstName= :firstName";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.setParameter("firstName", "Kuldeep");
        q.executeUpdate();

        UserInfo result = em.find(UserInfo.class, "mevivs_info");
        Assert.assertNull(result);
        em.close();
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
                logger.error("Error while truncating db",e);
            }
 
            catch (NoSuchFieldException e)
            {
                logger.error("Error while truncating db",e);
            }
            catch (IllegalArgumentException e)
            {
                logger.error("Error while truncating db",e);
            }
            catch (IllegalAccessException e)
            {
                logger.error("Error while truncating db",e);
            }
        }

    }

}
