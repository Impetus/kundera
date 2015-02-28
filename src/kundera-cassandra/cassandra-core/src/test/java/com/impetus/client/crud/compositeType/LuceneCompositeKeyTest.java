/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
import java.util.HashMap;
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

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * test case for lucenecompositekey
 * 
 * @author shaheed.hussain
 * 
 */
public class LuceneCompositeKeyTest
{
    private static final String PERSISTENCE_UNIT = "cassandra_cql";

    private static final String _keyspace = "CqlKeyspace";

    private static final String LUCENE_DIR_PATH = "./lucene";

    private static final Logger log = LoggerFactory.getLogger(LuceneCompositeKeyTest.class);

    private EntityManager em;

    private EntityManagerFactory emf;

    private Date currentDate = new Date();

    private Map<String, Object> puProperties = new HashMap<String, Object>();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        puProperties.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        puProperties.put("index.home.dir", "lucene");
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        CassandraCli.createKeySpace(_keyspace);
        
    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        CassandraCli.dropKeySpace(_keyspace);
        LuceneCleanupUtilities.cleanDir(LUCENE_DIR_PATH);
    }

    @Test
    public void compositeKeyQuerytest()
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT, puProperties);
        em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();

        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraPrimeUser user = new CassandraPrimeUser(key);
        user.setName("ankit");
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        CassandraCompoundKey key2 = new CassandraCompoundKey("shahid", 1, timeLineId);
        CassandraPrimeUser user2 = new CassandraPrimeUser(key2);
        user2.setTweetBody("my second tweet");
        user2.setTweetDate(currentDate);
        em.persist(user2);

        em.flush(); // optional,just to clear persistence cache.

        em.clear();

        CassandraPrimeUser u = em.find(CassandraPrimeUser.class, key);
        Assert.assertEquals("my first tweet", u.getTweetBody());

        em.clear();

        final String s = "Select u.name from CassandraPrimeUser u where u.name=ankit";
        Query qu = em.createQuery(s);
        List<CassandraPrimeUser> result = qu.getResultList();
        Assert.assertNotNull(result.get(0).getName());
        Assert.assertNull(result.get(0).getTweetBody());
        Assert.assertNull(result.get(0).getTweetDate());

        final String selectiveColumnTweetBodyWithAllCompositeColClause = "Select u from CassandraPrimeUser u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";

        // Query for selective column tweetBody with composite key clause.
        Query q = em.createQuery(selectiveColumnTweetBodyWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        List<CassandraPrimeUser> results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("my first tweet", results.get(0).getTweetBody());

        final String selectiveColumnTweetDateWithAllCompositeColClause = "Select u from CassandraPrimeUser u where u.key.userId = :userId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetDate with composite key clause.
        q = em.createQuery(selectiveColumnTweetDateWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(currentDate.getTime(), results.get(0).getTweetDate().getTime());

        final String querySearcingAllRecords = "Select u from CassandraPrimeUser u";
        // Query with composite key clause.
        q = em.createQuery(querySearcingAllRecords);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        final String withCompositeKeyClause = "Select u from CassandraPrimeUser u where u.key = :key";
        // Query with composite key clause.
        q = em.createQuery(withCompositeKeyClause);
        q.setParameter("key", key);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        em.flush(); // just to clear persistence cache.
        em.clear();

        em.remove(user);

        em.flush(); // optional,just to clear persistence cache.
        em.clear();

        final String querySearcingAfterDeletion = "Select u from CassandraPrimeUser u";
        // Query with composite key clause.
        q = em.createQuery(querySearcingAfterDeletion);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void compositeKeytest()
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT, puProperties);
        em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();

        CassandraCompoundKey key = new CassandraCompoundKey("shahid", 1, timeLineId);
        CassandraPrimeUser user = new CassandraPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        CassandraPrimeUser user2 = new CassandraPrimeUser(key);
        user2.setTweetBody("my second tweet");
        user2.setTweetDate(currentDate);

        em.flush(); // optional,just to clear persistence cache.

        em.clear();

        CassandraPrimeUser u = em.find(CassandraPrimeUser.class, key);
        Assert.assertEquals("my first tweet", u.getTweetBody());

        em.merge(user2);
        u = em.find(CassandraPrimeUser.class, key);
        Assert.assertEquals("my second tweet", u.getTweetBody());
        em.remove(user2);
    }

    @Test
    public void lucenePartitionKeyTest()
    {
        createMultiplePatitionKeyTable();
        puProperties.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "");
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT, puProperties);
        em = emf.createEntityManager();

        PartitionKey partitionKey1 = new PartitionKey();
        partitionKey1.setPartitionKey1("partitionKey1");
        partitionKey1.setPartitionKey2(1);

        IdWithMultiplePartitionKey id1 = new IdWithMultiplePartitionKey();
        id1.setClusterkey1("clusterkey1");
        id1.setClusterkey2(11);
        id1.setPartitionKey(partitionKey1);

        EntityWithMultiplePartitionKey entity1 = new EntityWithMultiplePartitionKey();
        entity1.setEntityDiscription("Entity to test composite key with multiple partition key.");
        entity1.setAction("Persisting");
        entity1.setId(id1);

        PartitionKey partitionKey2 = new PartitionKey();
        partitionKey2.setPartitionKey1("partitionKey2");
        partitionKey2.setPartitionKey2(2);

        IdWithMultiplePartitionKey id2 = new IdWithMultiplePartitionKey();
        id2.setClusterkey1("clusterkey2");
        id2.setClusterkey2(111);
        id2.setPartitionKey(partitionKey2);

        EntityWithMultiplePartitionKey entity2 = new EntityWithMultiplePartitionKey();
        entity2.setEntityDiscription("Entity to test composite key with multiple partition key.");
        entity2.setAction("Persisting");
        entity2.setId(id2);

        PartitionKey partitionKey3 = new PartitionKey();
        partitionKey3.setPartitionKey1("partitionKey3");
        partitionKey3.setPartitionKey2(3);

        IdWithMultiplePartitionKey id3 = new IdWithMultiplePartitionKey();
        id3.setClusterkey1("clusterkey3");
        id3.setClusterkey2(1111);
        id3.setPartitionKey(partitionKey3);

        EntityWithMultiplePartitionKey entity3 = new EntityWithMultiplePartitionKey();
        entity3.setEntityDiscription("Entity to test composite key with multiple partition key.");
        entity3.setAction("Persisting");
        entity3.setId(id3);

        // Insert.
        em.persist(entity1);
        em.persist(entity2);
        em.persist(entity3);

        em.clear();

        // Select All query.
        List<EntityWithMultiplePartitionKey> foundEntitys = em.createQuery(
                "select e from EntityWithMultiplePartitionKey e").getResultList();

        Assert.assertNotNull(foundEntitys);
        Assert.assertFalse(foundEntitys.isEmpty());
        Assert.assertEquals(3, foundEntitys.size());

        int count = 0;
        for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
        {
            Assert.assertNotNull(foundEntity);
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());

            if (foundEntity.getId().getClusterkey1().equals("clusterkey1"))
            {
                Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
                Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
            else if (foundEntity.getId().getClusterkey1().equals("clusterkey2"))
            {
                Assert.assertEquals(111, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey2", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(2, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
            else
            {
                Assert.assertEquals("clusterkey3", foundEntity.getId().getClusterkey1());
                Assert.assertEquals(1111, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey3", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(3, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
        }
        Assert.assertEquals(3, count);
        em.clear();

        // Select by both partition key query.
        foundEntitys = em
                .createQuery(
                        "select e from EntityWithMultiplePartitionKey e where e.id.partitionKey.partitionKey1=partitionKey1 and e.id.partitionKey.partitionKey2=1")
                .getResultList();

        Assert.assertNotNull(foundEntitys);
        Assert.assertFalse(foundEntitys.isEmpty());
        Assert.assertEquals(1, foundEntitys.size());

        count = 0;
        for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
        {
            Assert.assertNotNull(foundEntity);
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());
            Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
            Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
            Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
            Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
            count++;
        }
        Assert.assertEquals(1, count);

        // Select by both partition key query and one cluster key.
        foundEntitys = em
                .createQuery(
                        "select e from EntityWithMultiplePartitionKey e where e.id.partitionKey.partitionKey1=partitionKey1 and e.id.partitionKey.partitionKey2=1 and e.id.clusterkey1=clusterkey1")
                .getResultList();

        Assert.assertNotNull(foundEntitys);
        Assert.assertFalse(foundEntitys.isEmpty());
        Assert.assertEquals(1, foundEntitys.size());

        count = 0;
        for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
        {
            Assert.assertNotNull(foundEntity);
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());
            Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
            Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
            Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
            Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
            count++;
        }
        Assert.assertEquals(1, count);

        // Select by both partition key query.
        foundEntitys = em
                .createQuery("select e from EntityWithMultiplePartitionKey e where e.id.partitionKey = :partitionKey")
                .setParameter("partitionKey", partitionKey1).getResultList();

        Assert.assertNotNull(foundEntitys);
        Assert.assertFalse(foundEntitys.isEmpty());
        Assert.assertEquals(1, foundEntitys.size());

        count = 0;
        for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
        {
            Assert.assertNotNull(foundEntity);
            Assert.assertNotNull(foundEntity.getId());
            Assert.assertNotNull(foundEntity.getId().getPartitionKey());
            Assert.assertEquals("Persisting", foundEntity.getAction());
            Assert.assertEquals("Entity to test composite key with multiple partition key.",
                    foundEntity.getEntityDiscription());
            Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
            Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
            Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
            Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
            count++;
        }
        Assert.assertEquals(1, count);

        try
        {
            foundEntitys = em.createQuery("select e from EntityWithMultiplePartitionKey e where e.id = :id")
                    .setParameter("id", id1).getResultList();

            Assert.assertNotNull(foundEntitys);
            Assert.assertFalse(foundEntitys.isEmpty());
            Assert.assertEquals(1, foundEntitys.size());

            count = 0;
            for (EntityWithMultiplePartitionKey foundEntity : foundEntitys)
            {
                Assert.assertNotNull(foundEntity);
                Assert.assertNotNull(foundEntity.getId());
                Assert.assertNotNull(foundEntity.getId().getPartitionKey());
                Assert.assertEquals("Persisting", foundEntity.getAction());
                Assert.assertEquals("Entity to test composite key with multiple partition key.",
                        foundEntity.getEntityDiscription());
                Assert.assertEquals("clusterkey1", foundEntity.getId().getClusterkey1());
                Assert.assertEquals(11, foundEntity.getId().getClusterkey2());
                Assert.assertEquals("partitionKey1", foundEntity.getId().getPartitionKey().getPartitionKey1());
                Assert.assertEquals(1, foundEntity.getId().getPartitionKey().getPartitionKey2());
                count++;
            }
            Assert.assertEquals(1, count);
        }
        catch (Exception e)
        {
            Assert.fail();
        }

        // Select by first partition key query.
        try
        {
            foundEntitys = em
                    .createQuery(
                            "select e from EntityWithMultiplePartitionKey e where e.id.partitionKey.partitionKey1=partitionKey1")
                    .getResultList();

            Assert.fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals("Incomplete partition key fields in query", e.getMessage());
        }

    }
    
    private void createMultiplePatitionKeyTable() {
        try
        {
            CassandraCli
                    .executeCqlQuery(
                            "create table \"EntityWithMultiplePartitionKey\"(\"partitionKey1\" text, \"partitionKey2\" int, \"clusterkey1\" text, \"clusterkey2\" int, \"entityDiscription\" text, action text, PRIMARY KEY((\"partitionKey1\", \"partitionKey2\"), \"clusterkey1\", \"clusterkey2\"))",
                            _keyspace);
        }
        catch (Exception e)
        {
            log.warn(e.getMessage());
        }
    }
}
