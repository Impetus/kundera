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

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
/**
 * test case for lucenecompositekey
 * @author shaheed.hussain
 *
 */
public class LuceneCompositeKeyTest
{
    private static final String PERSISTENCE_UNIT = "luceneCassandraTest";

    private static final String LUCENE_DIR_PATH = "./lucene";

    private EntityManager em;

    private EntityManagerFactory emf;

    private Date currentDate = new Date();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        Map<String, Object> puProperties = new HashMap<String, Object>();
        puProperties.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT,puProperties);
        em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase) client).setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        
    }

    @SuppressWarnings("all")
    @Test
    public void compositeKeyQuerytest()
    {
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
        
        CassandraPrimeUser u=em.find(CassandraPrimeUser.class, key);
        Assert.assertEquals("my first tweet",u.getTweetBody());
        
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
        //LuceneCleanupUtilities.cleanDir(LUCENE_DIR_PATH);
    }
    
    @SuppressWarnings("all")
    @Test
    public void compositeKeytest()
    {
        

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
        
        CassandraPrimeUser u=em.find(CassandraPrimeUser.class, key);
        Assert.assertEquals("my first tweet",u.getTweetBody());
        
        em.merge(user2);
        u=em.find(CassandraPrimeUser.class, key);
        Assert.assertEquals("my second tweet",u.getTweetBody());
        em.remove(user2);
       // LuceneCleanupUtilities.cleanDir(LUCENE_DIR_PATH);
    }

}
