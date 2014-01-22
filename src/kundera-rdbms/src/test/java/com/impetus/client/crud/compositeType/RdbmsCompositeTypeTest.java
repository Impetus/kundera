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

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.RDBMSCli;
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * @author vivek.mishra
 * 
 */
public class RdbmsCompositeTypeTest
{
    private EntityManagerFactory emf;

    private RDBMSCli cli;
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        cli = new RDBMSCli("TESTDB");
        cli.createSchema("TESTDB");
        createSchema();
        emf = null;
        emf = Persistence.createEntityManagerFactory("testHibernate");
    }

    /**
     * CRUD over Compound primary Key.
     */
    @Test
    public synchronized void onCRUD()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        RdbmsCompoundKey key = new RdbmsCompoundKey("mevivs", 1, timeLineId.toString());
        RdbmsPrimeUser user = new RdbmsPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setUserName("kk");
        em.persist(user);

        em.close(); // optional,just to clear persistence cache.

        em = emf.createEntityManager();
        RdbmsPrimeUser result = em.find(RdbmsPrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("my first tweet", result.getTweetBody());
        Assert.assertEquals(timeLineId.toString(), result.getKey().getTimeLineId());
        Assert.assertEquals("kk", result.getUserName());

        em.clear();// optional,just to clear persistence cache.

        user.setTweetBody("After merge");
        em.merge(user);

        em.close();// optional,just to clear persistence cache.

        em = emf.createEntityManager();
        result = em.find(RdbmsPrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("After merge", result.getTweetBody());
        Assert.assertEquals(timeLineId.toString(), result.getKey().getTimeLineId());
        Assert.assertEquals("kk", result.getUserName());

        // deleting composite
        em.remove(result);

        em.close();// optional,just to clear persistence cache.

        em = emf.createEntityManager();
        result = em.find(RdbmsPrimeUser.class, key);
        Assert.assertNull(result);
    }

    @Test
    public synchronized void onQuery()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        RdbmsCompoundKey key = new RdbmsCompoundKey("mevivs", 1, timeLineId.toString());
        RdbmsPrimeUser user = new RdbmsPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setUserName("kk");
        em.persist(user);

        em.close(); // optional,just to clear persistence cache.
        em = emf.createEntityManager();
        final String noClause = "Select u from RdbmsPrimeUser u";

        final String withFirstCompositeColClause = "Select u from RdbmsPrimeUser u where u.key.userId = :userId";

        final String withClauseOnNoncomposite = "Select u from RdbmsPrimeUser u where u.userName = ?1";

        final String withSecondCompositeColClause = "Select u from RdbmsPrimeUser u where u.key.tweetId = :tweetId";
        final String withBothCompositeColClause = "Select u from RdbmsPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId";
        final String withAllCompositeColClause = "Select u from RdbmsPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        final String withLastCompositeColGTClause = "Select u from RdbmsPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId >= :timeLineId";

        // query with no clause.
        Query q = em.createQuery(noClause);
        List<RdbmsPrimeUser> results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withFirstCompositeColClause);
        q.setParameter("userId", "mevivs");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withClauseOnNoncomposite);
        q.setParameter(1, "kk");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withSecondCompositeColClause);
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withBothCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

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
        Assert.assertEquals(1, results.size());

        final String withCompositeKeyClause = "Select u from RdbmsPrimeUser u where u.key = :key";
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

        RdbmsCompoundKey key = new RdbmsCompoundKey("mevivs", 1, timeLineId.toString());
        RdbmsPrimeUser user = new RdbmsPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setUserName("kk");
        em.persist(user);

        em.close();

        em = emf.createEntityManager();

        String updateQuery = "Update RdbmsPrimeUser u SET u.tweetBody=after merge where u.tweetBody= :beforeUpdate";
        Query q = em.createQuery(updateQuery);
        q.setParameter("beforeUpdate", "my first tweet");
        q.executeUpdate();
        em.close();
        em = emf.createEntityManager();

        RdbmsPrimeUser result = em.find(RdbmsPrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("after merge", result.getTweetBody());
        Assert.assertEquals(timeLineId.toString(), result.getKey().getTimeLineId());
        Assert.assertEquals("kk", result.getUserName());
        em.close();
    }

    /**
     * delete by Named Query.
     */
    private void deleteNamed()
    {
        UUID timeLineId = UUID.randomUUID();

        RdbmsCompoundKey key = new RdbmsCompoundKey("mevivs", 1, timeLineId.toString());

        String deleteQuery = "Delete From RdbmsPrimeUser u where u.tweetBody= :tweetBody";
        EntityManager em = emf.createEntityManager();
        Query q = em.createQuery(deleteQuery);
        q.setParameter("tweetBody", "after merge");
        q.executeUpdate();

        em.close();
        em = emf.createEntityManager();
        RdbmsPrimeUser result = em.find(RdbmsPrimeUser.class, key);
        Assert.assertNull(result);
        em.close();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        dropSchema();
    }
    
    public void createSchema() throws SQLException
    {
        try
        {
            cli.createSchema("testdb");
            cli.update("CREATE TABLE TESTDB.USER (tweetBody VARCHAR(255), userName VARCHAR(255), tweetId INTEGER, userId VARCHAR(255), timeLineId VARCHAR(255)) ");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM TESTDB.USER");
            cli.update("DROP TABLE TESTDB.USER");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE TABLE TESTDB.USER (tweetBody VARCHAR(255), userName VARCHAR(255), tweetId INTEGER, userId VARCHAR(255), timeLineId VARCHAR(255)) ");
        }
    }

    public void dropSchema()
    {
        try
        {
            cli.update("DELETE FROM TESTDB.USER");
            cli.update("DROP TABLE TESTDB.USER");
            cli.update("DROP SCHEMA TESTDB");
            cli.closeConnection();
        }
        catch (Exception e)
        {
            // Nothing to do
        }
    }
}
