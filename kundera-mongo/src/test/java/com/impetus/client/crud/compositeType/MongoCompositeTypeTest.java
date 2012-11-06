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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.report.CSVSummaryReportModule;
import org.databene.contiperf.report.HtmlReportModule;
import org.databene.contiperf.report.ReportModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.mongodb.DB;

/**
 * @author vivek.mishra
 * 
 */

public class MongoCompositeTypeTest
{
    private EntityManagerFactory emf;

    private static final Log logger = LogFactory.getLog(MongoCompositeTypeTest.class);

    @Rule
    public ContiPerfRule i = new ContiPerfRule(new ReportModule[] { new CSVSummaryReportModule(),
            new HtmlReportModule() });

    private Date currentDate = new Date();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = null;
        emf = Persistence.createEntityManagerFactory("mongoTest");
    }

    /**
     * CRUD over Compound primary Key.
     */
    @Test
    @PerfTest(invocations = 10)
    public synchronized void onCRUD()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);
        MongoPrimeUser user = new MongoPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.clear(); // optional,just to clear persistence cache.

        MongoPrimeUser result = em.find(MongoPrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("my first tweet", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        em.clear();// optional,just to clear persistence cache.

        user.setTweetBody("After merge");
        em.merge(user);

        em.clear();// optional,just to clear persistence cache.

        result = em.find(MongoPrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("After merge", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        // deleting composite
        em.remove(result);

        em.clear();// optional,just to clear persistence cache.

        result = em.find(MongoPrimeUser.class, key);
        Assert.assertNull(result);
    }

    @Test
    public synchronized void onQuery()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();        
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);
        MongoPrimeUser user = new MongoPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.clear(); // optional,just to clear persistence cache.
        final String noClause = "Select u from MongoPrimeUser u";

        final String withFirstCompositeColClause = "Select u from MongoPrimeUser u where u.key.userId = :userId";

        final String withClauseOnNoncomposite = "Select u from MongoPrimeUser u where u.tweetDate = ?1";

        // NOSQL Intelligence to teach that query is invalid because partition
        // key is not present?
        final String withSecondCompositeColClause = "Select u from MongoPrimeUser u where u.key.tweetId = :tweetId";
        final String withBothCompositeColClause = "Select u from MongoPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId";
        final String withAllCompositeColClause = "Select u from MongoPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        final String withLastCompositeColGTClause = "Select u from MongoPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId >= :timeLineId";

        final String withSelectiveCompositeColClause = "Select u.key from MongoPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";

        // query over 1 composite and 1 non-column

        // query with no clause.
        Query q = em.createQuery(noClause);
        List<MongoPrimeUser> results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withFirstCompositeColClause);
        q.setParameter("userId", "mevivs");
        results = q.getResultList();
        Assert.assertNull(results);

        // Query with composite key clause.
        q = em.createQuery(withClauseOnNoncomposite);
        q.setParameter(1, currentDate);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withSecondCompositeColClause);
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertNull(results);

        // Query with composite key clause.
        q = em.createQuery(withBothCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertNull(results);

        // Query with composite key clause.
        q = em.createQuery(withAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withLastCompositeColGTClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        // TODO::
        // Assert.assertEquals(1, results.size());

        // Query with composite key with selective clause.
        q = em.createQuery(withSelectiveCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertNull(results.get(0).getTweetBody());

        final String selectiveColumnTweetBodyWithAllCompositeColClause = "Select u.tweetBody from MongoPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetBody with composite key clause.
        q = em.createQuery(selectiveColumnTweetBodyWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("my first tweet", results.get(0).getTweetBody());
        Assert.assertNull(results.get(0).getTweetDate());

        final String selectiveColumnTweetDateWithAllCompositeColClause = "Select u.tweetDate from MongoPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetDate with composite key clause.
        q = em.createQuery(selectiveColumnTweetDateWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(currentDate.getTime(), results.get(0).getTweetDate().getTime());
        Assert.assertNull(results.get(0).getTweetBody());

        final String withCompositeKeyClause = "Select u from MongoPrimeUser u where u.key = :key";
        // Query with composite key clause.
        q = em.createQuery(withCompositeKeyClause);
        q.setParameter("key", key);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        em.remove(user);

        em.clear();// optional,just to clear persistence cache.
    }

    @Test
    public synchronized void onNamedQueryTest()
    {
        updateNamed();
        deleteNamed();

    }

    /**
     * Update by Named Query.
     * 
     * @return
     */
    private void updateNamed()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);
        MongoPrimeUser user = new MongoPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em = emf.createEntityManager();

        String updateQuery = "Update MongoPrimeUser u SET u.tweetBody=after merge where u.tweetBody= :beforeUpdate";
        Query q = em.createQuery(updateQuery);
        q.setParameter("beforeUpdate", "my first tweet");
        q.executeUpdate();

        MongoPrimeUser result = em.find(MongoPrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("after merge", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());
        em.close();
    }

    /**
     * delete by Named Query.
     */
    private void deleteNamed()
    {
        UUID timeLineId = UUID.randomUUID();
        
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);

        String deleteQuery = "Delete From MongoPrimeUser u where u.tweetBody= :tweetBody";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.setParameter("tweetBody", "after merge");
        q.executeUpdate();

        MongoPrimeUser result = em.find(MongoPrimeUser.class, key);
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
