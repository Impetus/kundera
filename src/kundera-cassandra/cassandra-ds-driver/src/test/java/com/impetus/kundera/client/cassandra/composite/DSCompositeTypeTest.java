/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.client.cassandra.composite;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.composite.PrimeUser.NickName;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Junit test case for Compound/Composite key.
 * 
 * @author vivek.mishra
 * 
 */

public class DSCompositeTypeTest
{

    /**
     * 
     */
    private static final String PERSISTENCE_UNIT = "ds_composite_pu";

    private EntityManagerFactory emf;

    private Date currentDate = new Date();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();

    }

    @Test
    public void onMultipleRecords()
    {
        String cql_Query = "create columnfamily \"CompositeUser\" (\"userId\" text, \"tweetId\" int, \"timeLineId\" uuid, "
                + " \"tweetDate\" timestamp, PRIMARY KEY(\"userId\",\"tweetId\",\"timeLineId\"))";
        executeScript(cql_Query);
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);

        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        UserTimeLine key1 = new UserTimeLine("mevivs", 1, timeLineId);
        PrimeUser user1 = new PrimeUser(key1);
        user1.setTweetBody("my first tweet");
        user1.setTweetDate(currentDate);
        user1.setNickName(NickName.KK);

        timeLineId = UUID.randomUUID();
        UserTimeLine key2 = new UserTimeLine("mevivs", 1, timeLineId);
        PrimeUser user2 = new PrimeUser(key2);
        user2.setTweetBody("my first tweet");
        user2.setTweetDate(currentDate);
        user2.setNickName(NickName.KK);

        em.persist(user1);
        em.persist(user2);

        em.clear();

        final String noClause = "Select u from PrimeUser u";
        Query query = em.createQuery(noClause);

        List<PrimeUser> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertNotSame(results.get(0).getKey(), results.get(1).getKey());
        Assert.assertEquals(results.get(0).getKey().getUserId(), results.get(1).getKey().getUserId());
        Assert.assertNotSame(results.get(0).getKey().getTimeLineId(), results.get(1).getKey().getTimeLineId());
        Assert.assertEquals(results.get(0).getTweetBody(), results.get(1).getTweetBody());

        final String deleteQuery = "Delete from PrimeUser p";
        em.createQuery(deleteQuery).executeUpdate();
        query = em.createQuery(noClause);
        results = query.getResultList();

        Assert.assertTrue(results.isEmpty());

        em.close();
        // emf.close();
    }

    @Test
    public void onAddColumn() throws Exception
    {
        // cql script is not adding "tweetBody", kundera.ddl.auto.update will
        // add it.
        String cql_Query = "create columnfamily \"CompositeUser\" (\"userId\" text, \"tweetId\" int, \"timeLineId\" uuid, "
                + " \"tweetDate\" timestamp, PRIMARY KEY(\"userId\",\"tweetId\",\"timeLineId\"))";

        executeScript(cql_Query);
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);

        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        UserTimeLine key = new UserTimeLine("mevivs", 1, timeLineId);
        onCRUD(em, key);
        em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");
        PrimeUser result = em.find(PrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("After merge", result.getTweetBody()); // assertion
                                                                   // of newly
                                                                   // added
                                                                   // tweet body
        em.remove(result);

        em.flush();
        em.close();
        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        result = em.find(PrimeUser.class, key);
        Assert.assertNull(result);

        em.close();
    }

    // @Test
    public void onAlterColumnType() throws Exception
    {
        // Here tweetDate is of type "int". On update will be changed to
        // timestamp.

        String cql_Query = "create columnfamily \"CompositeUser\" (\"userId\" text, \"tweetId\" int, \"timeLineId\" uuid, "
                + " \"tweetDate\" int, PRIMARY KEY(\"userId\",\"tweetId\",\"timeLineId\"))";
        executeScript(cql_Query);
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        EntityManager em = emf.createEntityManager();
        UUID timeLineId = UUID.randomUUID();
        UserTimeLine key = new UserTimeLine("mevivs", 1, timeLineId);
        onCRUD(em, key);
        em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        PrimeUser result = em.find(PrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime()); // assertion
                                                                                     // of
                                                                                     // changed
                                                                                     // tweetDate.
        em.remove(result);
        em.flush();
        em.close();
        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");
        result = em.find(PrimeUser.class, key);
        Assert.assertNull(result);

        em.close();
    }

    @Test
    public void onQuery()
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();

        UserTimeLine key = new UserTimeLine("mevivs", 1, timeLineId);

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");

        PrimeUser user = new PrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.flush(); // optional,just to clear persistence cache.

        em.clear();

        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        final String noClause = "Select u from PrimeUser u";

        final String withFirstCompositeColClause = "Select u from PrimeUser u where u.key.userId = :userId";

        // secondary index support over compound key is not enabled in cassandra
        // composite keys yet. DO NOT DELETE/UNCOMMENT.

        // final String withClauseOnNoncomposite =
        // "Select u from CassandraPrimeUser u where u.tweetDate = ?1";
        //

        final String withSecondCompositeColClause = "Select u from PrimeUser u where u.key.tweetId = :tweetId";
        final String withBothCompositeColClause = "Select u from PrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId";
        final String withAllCompositeColClause = "Select u from PrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        final String withLastCompositeColGTClause = "Select u from PrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId >= :timeLineId";

        final String withSelectiveCompositeColClause = "Select u.key from PrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";

        // query over 1 composite and 1 non-column

        // query with no clause.
        Query q = em.createQuery(noClause);
        List<PrimeUser> results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withFirstCompositeColClause);
        q.setParameter("userId", "mevivs");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());

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

        final String selectiveColumnTweetBodyWithAllCompositeColClause = "Select u.tweetBody from PrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetBody with composite key clause.
        q = em.createQuery(selectiveColumnTweetBodyWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("my first tweet", results.get(0).getTweetBody());
        Assert.assertNull(results.get(0).getTweetDate());

        final String selectiveColumnTweetDateWithAllCompositeColClause = "Select u.tweetDate from PrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetDate with composite key clause.
        q = em.createQuery(selectiveColumnTweetDateWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(currentDate.getTime(), results.get(0).getTweetDate().getTime());
        Assert.assertNull(results.get(0).getTweetBody());

        final String withCompositeKeyClause = "Select u from PrimeUser u where u.key = :key";
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
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        updateNamed();
        deleteNamed();
    }

    @Test
    public void onLimit()
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        UserTimeLine key = new UserTimeLine("mevivs", 1, timeLineId);
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        PrimeUser user1 = new PrimeUser(key);
        user1.setTweetBody("my first tweet");
        user1.setTweetDate(currentDate);
        em.persist(user1);

        key = new UserTimeLine("mevivs", 2, timeLineId);
        PrimeUser user2 = new PrimeUser(key);
        user2.setTweetBody("my first tweet");
        user2.setTweetDate(currentDate);
        em.persist(user2);

        key = new UserTimeLine("mevivs", 3, timeLineId);
        PrimeUser user3 = new PrimeUser(key);
        user3.setTweetBody("my first tweet");
        user3.setTweetDate(currentDate);
        em.persist(user3);

        em.flush();

        // em.clear(); // optional,just to clear persistence cache.

        // em = emf.createEntityManager();
        em.clear();

        final String noClause = "Select u from PrimeUser u";
        Query q = em.createQuery(noClause);
        List<PrimeUser> results = q.getResultList();
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
    public void onOrderBYClause()
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        UserTimeLine key = new UserTimeLine("mevivs", 1, timeLineId);
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        PrimeUser user1 = new PrimeUser(key);
        user1.setTweetBody("my first tweet");
        user1.setTweetDate(currentDate);
        em.persist(user1);

        key = new UserTimeLine("mevivs", 2, timeLineId);
        PrimeUser user2 = new PrimeUser(key);
        user2.setTweetBody("my second tweet");
        user2.setTweetDate(currentDate);
        em.persist(user2);

        key = new UserTimeLine("mevivs", 3, timeLineId);
        PrimeUser user3 = new PrimeUser(key);
        user3.setTweetBody("my third tweet");
        user3.setTweetDate(currentDate);
        em.persist(user3);

        em.flush();

        // em.clear(); // optional,just to clear persistence cache.

        // em = emf.createEntityManager();
        em.clear();

        String orderClause = "Select u from PrimeUser u where u.key.userId = :userId ORDER BY u.key.tweetId ASC";
        Query q = em.createQuery(orderClause);
        q.setParameter("userId", "mevivs");
        List<PrimeUser> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals("my first tweet", results.get(0).getTweetBody());
        Assert.assertEquals("my second tweet", results.get(1).getTweetBody());
        Assert.assertEquals("my third tweet", results.get(2).getTweetBody());
        Assert.assertEquals(3, results.size());

        orderClause = "Select u from PrimeUser u where u.key.userId = :userId ORDER BY u.key.tweetId DESC";
        q = em.createQuery(orderClause);
        q.setParameter("userId", "mevivs");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals("my first tweet", results.get(2).getTweetBody());
        Assert.assertEquals("my second tweet", results.get(1).getTweetBody());
        Assert.assertEquals("my third tweet", results.get(0).getTweetBody());
        Assert.assertEquals(3, results.size());

        // With limit
        q = em.createQuery(orderClause);
        q.setParameter("userId", "mevivs");
        q.setMaxResults(2);
        results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        try
        {
            orderClause = "Select u from PrimeUser u where u.key.userId = :userId ORDER BY u.key.userId DESC";
            q = em.createQuery(orderClause);
            q.setParameter("userId", "mevivs");
            results = q.getResultList();

            Assert.fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals(
                    "com.datastax.driver.core.exceptions.InvalidQueryException: Order by is currently only supported on the clustered columns of the PRIMARY KEY, got userId",
                    e.getMessage());
        }

    }

    @Test
    public void onInClause()
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        UserTimeLine key = new UserTimeLine("mevivs", 1, timeLineId);
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        PrimeUser user1 = new PrimeUser(key);
        user1.setTweetBody("my first tweet");
        user1.setTweetDate(currentDate);
        em.persist(user1);

        key = new UserTimeLine("cgangwal's", 2, timeLineId);
        PrimeUser user2 = new PrimeUser(key);
        user2.setTweetBody("my second tweet");
        user2.setTweetDate(currentDate);
        em.persist(user2);

        key = new UserTimeLine("kmishra", 3, timeLineId);
        PrimeUser user3 = new PrimeUser(key);
        user3.setTweetBody("my third tweet");
        user3.setTweetDate(currentDate);
        em.persist(user3);

        em.flush();

        em.clear();

        // Single quote within single quote not supported by eclipselink, need
        // to look into this
        // String inClause =
        // "Select u from PrimeUser u where u.key.userId IN ('mevivs','cgangwal\''s','kmishra') ORDER BY u.key.tweetId ASC";
        // Query q = em.createQuery(inClause);
        //
        // List<PrimeUser> results = q.getResultList();
        // Assert.assertNotNull(results);
        // Assert.assertEquals(3, results.size());
        // Assert.assertEquals("my first tweet", results.get(0).getTweetBody());
        // Assert.assertEquals("my second tweet",
        // results.get(1).getTweetBody());
        // Assert.assertEquals("my third tweet", results.get(2).getTweetBody());
        //
        //
        String inClause = "Select u from PrimeUser u where u.key.userId IN (\"mevivs\",\"cgangwal's\",\"kmishra\")";
        Query q = em.createQuery(inClause);

        List<PrimeUser> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

        inClause = "Select u from PrimeUser u where u.key.userId IN (\"mevivs\",\"cgangwal's\")";
        q = em.createQuery(inClause);

        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        try
        {
            inClause = "Select u from PrimeUser u where u.key.tweetId IN (1,2,3) ORDER BY u.key.tweetId DESC";
            q = em.createQuery(inClause);
            results = q.getResultList();
            Assert.fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals(
                    "com.datastax.driver.core.exceptions.InvalidQueryException: ORDER BY is only supported when the partition key is restricted by an EQ or an IN.",
                    e.getMessage());
        }

    }

    @Test
    public void onBatchInsert()
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();

        for (int i = 0; i < 500; i++)
        {
            UserTimeLine key = new UserTimeLine("mevivs", i, timeLineId);
            Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
            Client client = clients.get(PERSISTENCE_UNIT);
            ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
            PrimeUser user = new PrimeUser(key);
            user.setTweetBody("my first tweet");
            user.setTweetDate(currentDate);
            em.persist(user);
        }

        em.clear();

        PrimeUser u = em.find(PrimeUser.class, new UserTimeLine("mevivs", 499, timeLineId));
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
        try
        {
            emf.close();
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("use \"KunderaExamples\"".getBytes()),
                    Compression.NONE, ConsistencyLevel.ONE);
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("truncate \"CompositeUser\"".getBytes()),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        finally
        {
            CassandraCli.dropKeySpace("KunderaExamples");
        }
    }

    // DO NOT DELETE IT!! though it is automated with schema creation option.
    /**
     * create column family script for compound key.
     */
    private void executeScript(final String cql)
    {
        CassandraCli.dropKeySpace("KunderaExamples");
        CassandraCli.createKeySpace("KunderaExamples");
        CassandraCli.executeCqlQuery(cql, "KunderaExamples");
    }

    /**
     * CRUD over Compound primary Key.
     */

    private void onCRUD(final EntityManager em, UserTimeLine key)
    {

        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        PrimeUser user = new PrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        user.setNickName(NickName.KK);
        em.persist(user);
        em.flush();

        // em.clear(); // optional,just to clear persistence cache.

        // em = emf.createEntityManager();
        em.clear();

        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        PrimeUser result = em.find(PrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("my first tweet", result.getTweetBody());
        // Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());
        Assert.assertEquals(NickName.KK, result.getNickName());
        Assert.assertEquals(NickName.KK.name(), result.getNickName().name());

        em.clear();// optional,just to clear persistence cache.

        user.setTweetBody("After merge");
        em.merge(user);
        em.close();// optional,just to clear persistence cache.

        EntityManager em1 = emf.createEntityManager();
        // em = emf.createEntityManager();
        clients = (Map<String, Client>) em1.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        result = em1.find(PrimeUser.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("After merge", result.getTweetBody());
        // Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        // deleting composite
        em1.clear(); // optional,just to clear persistence cache.
        // em.close();// optional,just to clear persistence cache.

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

        UserTimeLine key = new UserTimeLine("mevivs", 1, timeLineId);
        PrimeUser user = new PrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.close();
        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");

        String updateQuery = "Update PrimeUser u SET u.tweetBody='after merge' where u.key= :beforeUpdate";
        Query q = em.createQuery(updateQuery);
        q.setParameter("beforeUpdate", key);
        q.executeUpdate();

        em.close(); // optional,just to clear persistence cache.

        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        PrimeUser result = em.find(PrimeUser.class, key);
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

        UserTimeLine key = new UserTimeLine("mevivs", 1, timeLineId);

        String deleteQuery = "Delete From PrimeUser u where u.key= :key";
        EntityManager em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion("3.0.0");

        Query q = em.createQuery(deleteQuery);
        q.setParameter("key", key);
        q.executeUpdate();

        PrimeUser result = em.find(PrimeUser.class, key);
        Assert.assertNull(result);
        em.close();
    }
}
