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
package com.impetus.client.hbase.crud.compositetype;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class HBaseCompositeTypeTest.
 * 
 * @author vivek.mishra
 */

public class HBaseCompositeTypeTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "queryTest";

    /** The Constant SCHEMA. */
    private static final String SCHEMA = "HBaseNew";

    /** The em. */
    private EntityManager em;

    /** The time line id. */
    private UUID timeLineId;

    /** The current date. */
    private Date currentDate = new Date();

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * CRUD over Compound primary Key.
     */
    @Test
    public void onCRUD()
    {
        timeLineId = UUID.randomUUID();
        HBaseCompoundKey key = new HBaseCompoundKey("mevivs", 1, timeLineId);
        HBasePrimeUser user = new HBasePrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.close(); // optional,just to clear persistence cache.
        em = emf.createEntityManager();

        HBasePrimeUser result = em.find(HBasePrimeUser.class, key);
        validatePrimeUser(result);

        em.clear();

        user.setTweetBody("After merge");
        em.merge(user);

        em.clear();
        result = em.find(HBasePrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("After merge", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        em.remove(result);
        em.clear();

        result = em.find(HBasePrimeUser.class, key);
        Assert.assertNull(result);
    }

    /**
     * On query.
     */
    @Test
    public void onQuery()
    {
        timeLineId = UUID.randomUUID();
        HBaseCompoundKey key = new HBaseCompoundKey("mevivs", 1, timeLineId);
        HBasePrimeUser user = new HBasePrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        final String noClause = "Select u from HBasePrimeUser u";
        Query q = em.createQuery(noClause);
        List results = q.getResultList();
        Assert.assertEquals(1, results.size());
        validatePrimeUser(results.get(0));

        final String withFirstCompositeColClause = "Select u from HBasePrimeUser u where u.key.userId = :userId";
        q = em.createQuery(withFirstCompositeColClause);
        q.setParameter("userId", "mevivs");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validatePrimeUser(results.get(0));

        final String withClauseOnNoncomposite = "Select u from HBasePrimeUser u where u.tweetDate = ?1";
        q = em.createQuery(withClauseOnNoncomposite);
        q.setParameter(1, currentDate);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        validatePrimeUser(results.get(0));

        final String withSecondCompositeColClause = "Select u from HBasePrimeUser u where u.key.tweetId = :tweetId";
        q = em.createQuery(withSecondCompositeColClause);
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        final String withBothCompositeColClause = "Select u from HBasePrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId";
        q = em.createQuery(withBothCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validatePrimeUser(results.get(0));

        final String withAllCompositeColClause = "Select u from HBasePrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        q = em.createQuery(withAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validatePrimeUser(results.get(0));

        final String withLastCompositeColGTClause = "Select u from HBasePrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId >= :timeLineId";
        q = em.createQuery(withLastCompositeColGTClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        validatePrimeUser(results.get(0));

        final String withCompositeKeyClause = "Select u from HBasePrimeUser u where u.key = :key";
        q = em.createQuery(withCompositeKeyClause);
        q.setParameter("key", key);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validatePrimeUser(results.get(0));

        final String withSelectiveCompositeColClause = "Select u.key from HBasePrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        q = em.createQuery(withSelectiveCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("mevivs", ((HBaseCompoundKey) results.get(0)).getUserId());
        Assert.assertEquals(1, ((HBaseCompoundKey) results.get(0)).getTweetId());
        Assert.assertEquals(timeLineId, ((HBaseCompoundKey) results.get(0)).getTimeLineId());

        final String withSelectiveNestedCompositeColClause = "Select u.key.userId from HBasePrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        q = em.createQuery(withSelectiveNestedCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("mevivs", results.get(0));

        em.remove(user);
        em.clear();// optional,just to clear persistence cache.
    }

    /**
     * On named query test.
     */
    @Test
    public void onNamedQueryTest()
    {
        updateNamed();
        deleteNamed();

    }

    /**
     * Update by Named Query.
     * 
     * 
     */
    private void updateNamed()
    {
        EntityManager em = emf.createEntityManager();

        timeLineId = UUID.randomUUID();

        HBaseCompoundKey key = new HBaseCompoundKey("mevivs", 1, timeLineId);
        HBasePrimeUser user = new HBasePrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.close();

        em = emf.createEntityManager();

        String updateQuery = "Update HBasePrimeUser u SET u.tweetBody= 'after merge' where u.tweetBody= :beforeUpdate";
        Query q = em.createQuery(updateQuery);
        q.setParameter("beforeUpdate", "my first tweet");
        q.executeUpdate();

        em.close();
        em = emf.createEntityManager();

        HBasePrimeUser result = em.find(HBasePrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("after merge", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());
    }

    /**
     * delete by Named Query.
     */
    private void deleteNamed()
    {
        timeLineId = UUID.randomUUID();

        HBaseCompoundKey key = new HBaseCompoundKey("mevivs", 1, timeLineId);

        String deleteQuery = "Delete From HBasePrimeUser u where u.tweetBody= :tweetBody";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.setParameter("tweetBody", "after merge");
        q.executeUpdate();

        em.close();
        em = emf.createEntityManager();
        HBasePrimeUser result = em.find(HBasePrimeUser.class, key);
        Assert.assertNull(result);
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        EntityManager em = emf.createEntityManager();
        em.close();
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
        emf.close();
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

    /**
     * Assert prime user.
     * 
     * @param result
     *            the result
     */
    private void validatePrimeUser(Object result)
    {
        HBasePrimeUser user = (HBasePrimeUser) result;
        Assert.assertNotNull(user);
        Assert.assertEquals("my first tweet", user.getTweetBody());
        Assert.assertEquals(currentDate, user.getTweetDate());
        Assert.assertEquals(currentDate.getTime(), user.getTweetDate().getTime());
        Assert.assertNotNull(user.getKey());
        Assert.assertEquals("mevivs", user.getKey().getUserId());
        Assert.assertEquals(1, user.getKey().getTweetId());
        Assert.assertEquals(timeLineId, user.getKey().getTimeLineId());
    }
}
