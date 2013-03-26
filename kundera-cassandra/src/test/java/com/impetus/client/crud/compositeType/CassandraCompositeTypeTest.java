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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.report.CSVSummaryReportModule;
import org.databene.contiperf.report.HtmlReportModule;
import org.databene.contiperf.report.ReportModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.client.Client;

/**
 * Junit test case for Compound/Composite key.
 * 
 * @author vivek.mishra
 * 
 */

public class CassandraCompositeTypeTest
{

    /**
     * 
     */
    private static final String PERSISTENCE_UNIT = "composite_pu";

    private EntityManagerFactory emf;

    /** The Constant logger. */
    private static final Log logger = LogFactory.getLog(CassandraCompositeTypeTest.class);

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
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        CassandraCli.dropKeySpace("CompositeCassandra");
        // loadData();
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
    }

    /**
     * CRUD over Compound primary Key.
     */
    @Test
    @PerfTest(invocations = 10)
    public void onCRUD()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        CassandraPrimeUser user = new CassandraPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);
        em.flush();

        // em.clear(); // optional,just to clear persistence cache.

        // em = emf.createEntityManager();
        em.clear();

        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        CassandraPrimeUser result = em.find(CassandraPrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("my first tweet", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        em.clear();// optional,just to clear persistence cache.

        user.setTweetBody("After merge");
        em.merge(user);

        em.close();// optional,just to clear persistence cache.

        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        result = em.find(CassandraPrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("After merge", result.getTweetBody());
        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        // deleting composite
        em.remove(result);

        em.close();// optional,just to clear persistence cache.

        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        result = em.find(CassandraPrimeUser.class, key);
        Assert.assertNull(result);
    }

    @Test
    public void onQuery()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();

        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");

        CassandraPrimeUser user = new CassandraPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.flush(); // optional,just to clear persistence cache.

        em.clear();

        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        final String noClause = "Select u from CassandraPrimeUser u";

        final String withFirstCompositeColClause = "Select u from CassandraPrimeUser u where u.key.userId = :userId";

        // secondary index support over compound key is not enabled in cassandra
        // composite keys yet. DO NOT DELETE/UNCOMMENT.

        // final String withClauseOnNoncomposite =
        // "Select u from CassandraPrimeUser u where u.tweetDate = ?1";
        //

        final String withSecondCompositeColClause = "Select u from CassandraPrimeUser u where u.key.tweetId = :tweetId";
        final String withBothCompositeColClause = "Select u from CassandraPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId";
        final String withAllCompositeColClause = "Select u from CassandraPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        final String withLastCompositeColGTClause = "Select u from CassandraPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId >= :timeLineId";

        final String withSelectiveCompositeColClause = "Select u.key from CassandraPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";

        // query over 1 composite and 1 non-column

        // query with no clause.
        Query q = em.createQuery(noClause);
        List<CassandraPrimeUser> results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withFirstCompositeColClause);
        q.setParameter("userId", "mevivs");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        /*
         * // secondary index support over compound key is not enabled in
         * cassandra composite keys yet. DO NOT DELETE/UNCOMMENT.
         * 
         * // Query with composite key clause. q =
         * em.createQuery(withClauseOnNoncomposite); q.setParameter(1,
         * currentDate); results = q.getResultList(); Assert.assertEquals(1,
         * results.size());
         */
        // Query with composite key clause.
        q = em.createQuery(withSecondCompositeColClause);
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withBothCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        results = q.getResultList();
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

        // Query with composite key with selective clause.
        q = em.createQuery(withSelectiveCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertNull(results.get(0).getTweetBody());

        final String selectiveColumnTweetBodyWithAllCompositeColClause = "Select u.tweetBody from CassandraPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetBody with composite key clause.
        q = em.createQuery(selectiveColumnTweetBodyWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("my first tweet", results.get(0).getTweetBody());
        Assert.assertNull(results.get(0).getTweetDate());

        final String selectiveColumnTweetDateWithAllCompositeColClause = "Select u.tweetDate from CassandraPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetDate with composite key clause.
        q = em.createQuery(selectiveColumnTweetDateWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(currentDate.getTime(), results.get(0).getTweetDate().getTime());
        Assert.assertNull(results.get(0).getTweetBody());

        final String withCompositeKeyClause = "Select u from CassandraPrimeUser u where u.key = :key";
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
    public void onNamedQueryTest()
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

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");

        UUID timeLineId = UUID.randomUUID();

        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraPrimeUser user = new CassandraPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.close();
        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");

        String updateQuery = "Update CassandraPrimeUser u SET u.tweetBody=after merge where u.key= :beforeUpdate";
        Query q = em.createQuery(updateQuery);
        q.setParameter("beforeUpdate", key);
        q.executeUpdate();

        em.close(); // optional,just to clear persistence cache.

        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        CassandraPrimeUser result = em.find(CassandraPrimeUser.class, key);
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

        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);

        String deleteQuery = "Delete From CassandraPrimeUser u where u.key= :key";
        EntityManager em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");

        Query q = em.createQuery(deleteQuery);
        q.setParameter("key", key);
        q.executeUpdate();

        CassandraPrimeUser result = em.find(CassandraPrimeUser.class, key);
        Assert.assertNull(result);
        em.close();
    }

    @Test
    public void onLimit()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        CassandraPrimeUser user1 = new CassandraPrimeUser(key);
        user1.setTweetBody("my first tweet");
        user1.setTweetDate(currentDate);
        em.persist(user1);

        key = new CassandraCompoundKey("mevivs", 2, timeLineId);
        CassandraPrimeUser user2 = new CassandraPrimeUser(key);
        user2.setTweetBody("my first tweet");
        user2.setTweetDate(currentDate);
        em.persist(user2);

        key = new CassandraCompoundKey("mevivs", 3, timeLineId);
        CassandraPrimeUser user3 = new CassandraPrimeUser(key);
        user3.setTweetBody("my first tweet");
        user3.setTweetDate(currentDate);
        em.persist(user3);

        em.flush();

        // em.clear(); // optional,just to clear persistence cache.

        // em = emf.createEntityManager();
        em.clear();
        

        final String noClause = "Select u from CassandraPrimeUser u";
        Query q = em.createQuery(noClause);
        List<CassandraPrimeUser> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

        // With limit
        q = em.createQuery(noClause);
        q.setMaxResults(2);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

    }
    @Test
    public void onBatchInsert()
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 5000; i++)
        {
            CassandraCompoundKey key = new CassandraCompoundKey("mevivs", i, timeLineId);
            Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
            Client client = clients.get(PERSISTENCE_UNIT);
            ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
            CassandraPrimeUser user = new CassandraPrimeUser(key);
            user.setTweetBody("my first tweet");
            user.setTweetDate(currentDate);
            em.persist(user);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Total time taken = " + (t2 - t1));

        em.clear();

        CassandraPrimeUser u = em.find(CassandraPrimeUser.class, new CassandraCompoundKey("mevivs", 4999, timeLineId));
        Assert.assertNotNull(u);
    }

    /**
     * CompositeUserDataType
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        // emf.close();
        CassandraCli.dropKeySpace("CompositeCassandra");
    }

    // DO NOT DELETE IT!! though it is automated with schema creation option.
    /**
     * create column family script for compound key.
     */
    private void loadData()
    {
        /*
         * if (!CassandraCli.keyspaceExist("CompositeCassandra")) {
         */
        CassandraCli.createKeySpace("CompositeCassandra");

        String cql_Query = "create columnfamily \"CompositeUser\" (\"userId\" text, \"tweetId\" int, \"timeLineId\" uuid, \"tweetBody\" text,"
                + " \"tweetDate\" timestamp, PRIMARY KEY(\"userId\",\"tweetId\",\"timeLineId\"))";
        try
        {
            CassandraCli.getClient().set_keyspace("CompositeCassandra");
        }
        catch (InvalidRequestException e)
        {
            logger.error(e.getMessage());
        }
        catch (TException e)
        {
            logger.error(e.getMessage());
        }
        /*
         * if (!CassandraCli.columnFamilyExist("CompositeCassandra",
         * "CompositeUser")) {
         */
        CassandraCli.executeCqlQuery(cql_Query);
        // }
        //
        // }
    }

}
