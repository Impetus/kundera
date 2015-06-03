package com.impetus.client.couchdb.crud;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.couchdb.entities.CouchDBCompoundKey;
import com.impetus.client.couchdb.entities.CouchDBPrimeUser;

public class CouchDBCompositeKeyTest
{

    /** The Constant REDIS_PU. */
    private static final String COUCHDB_PU = "couchdb_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CouchDBCompositeKeyTest.class);

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(COUCHDB_PU);
    }

    @Test
    public void testCRUD()
    {
        logger.info("On testCRUD");
        EntityManager em = emf.createEntityManager();
        final String userId = "1";
        final int tweetId = 12;
        final UUID timeLineId = UUID.randomUUID();
        final Date tweetDate = new Date();
        CouchDBCompoundKey compoundKey = new CouchDBCompoundKey(userId, tweetId, timeLineId);
        CouchDBPrimeUser user = new CouchDBPrimeUser(compoundKey);
        user.setTweetBody("My tweet");
        user.setTweetDate(tweetDate);
        em.persist(user);

        em.clear(); // clear cache.

        CouchDBPrimeUser found = em.find(CouchDBPrimeUser.class, compoundKey);
        Assert.assertNotNull(found);
        Assert.assertNotNull(found.getKey());
        Assert.assertEquals(userId, found.getKey().getUserId());
        Assert.assertEquals(tweetDate, found.getTweetDate());
        em.remove(found);

        em.clear(); // clear cache.

        found = em.find(CouchDBPrimeUser.class, compoundKey);
        Assert.assertNull(found);
    }

    @Test
    public void testQuery()
    {
        logger.info("On testCRUD");
        EntityManager em = emf.createEntityManager();
        final String userId = "1";
        final int tweetId = 12;
        final UUID timeLineId = UUID.randomUUID();
        final Date tweetDate = new Date();
        CouchDBCompoundKey compoundKey = new CouchDBCompoundKey(userId, tweetId, timeLineId);
        CouchDBPrimeUser user = new CouchDBPrimeUser(compoundKey);
        user.setTweetBody("My tweet");
        user.setTweetDate(tweetDate);
        em.persist(user);

        em.clear(); // clear cache.

        javax.persistence.Query q = em.createQuery("select u from CouchDBPrimeUser u where u.key=:compoundKey");
        q.setParameter("compoundKey", compoundKey);
        List<CouchDBPrimeUser> users = q.getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());

        em.clear(); // clear cache.

        final String withAllCompositeColClause = "Select u from CouchDBPrimeUser u where u.key.user_Id = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        q = em.createQuery(withAllCompositeColClause);
        q.setParameter("userId", "1");
        q.setParameter("tweetId", 12);
        q.setParameter("timeLineId", timeLineId);
        users = q.getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());

        try
        {
            final String withOutAllCompositeColClause = "Select u from CouchDBPrimeUser u where u.key.user_Id = :userId and u.key.tweetId = :tweetId";
            q = em.createQuery(withOutAllCompositeColClause);
            q.setParameter("userId", "1");
            q.setParameter("tweetId", 12);
            users = q.getResultList();
        }
        catch (Exception e)
        {
            Assert.assertEquals(
                    "com.impetus.kundera.query.QueryHandlerException: There should be each and every field of composite key.",
                    e.getMessage());
        }

        CouchDBPrimeUser found = em.find(CouchDBPrimeUser.class, compoundKey);
        Assert.assertNotNull(found);
        Assert.assertNotNull(found.getKey());
        Assert.assertEquals(userId, found.getKey().getUserId());
        Assert.assertEquals(tweetDate, found.getTweetDate());
        em.remove(found);

        em.clear(); // clear cache.

        found = em.find(CouchDBPrimeUser.class, compoundKey);
        Assert.assertNull(found);
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

}
