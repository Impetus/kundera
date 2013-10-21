package com.impetus.client.couchdb.crud;

import java.util.Date;
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
import com.impetus.client.couchdb.entities.CouchDBEmbeddedUser;

/**
 * The Class RedisEmbeddableTest.
 */
public class CouchDBEmbeddableTest
{

    /** The Constant REDIS_PU. */
    private static final String COUCHDB_PU = "couchdb_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CouchDBCompositeKeyTest.class);

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(COUCHDB_PU);
    }

    /**
     * Test crud.
     */
    @Test
    public void testCRUD()
    {
        logger.info("On testCRUD");
        EntityManager em = emf.createEntityManager();
        final String userId = "1";
        final int tweetId = 12;
        final UUID timeLineId = UUID.randomUUID();
        final Date tweetDate = new Date();
        CouchDBCompoundKey embeddable = new CouchDBCompoundKey(userId, tweetId, timeLineId);
        CouchDBEmbeddedUser user = new CouchDBEmbeddedUser(userId);
        user.setTweetBody("My tweet");
        user.setTweetDate(tweetDate);
        user.setEmbeddable(embeddable);
        em.persist(user);

        em.clear(); // clear cache.

        CouchDBEmbeddedUser found = em.find(CouchDBEmbeddedUser.class, userId);
        Assert.assertNotNull(found);
        Assert.assertNotNull(found.getUserId());
        Assert.assertEquals(userId, found.getEmbeddable().getUserId());
        Assert.assertEquals(tweetDate, found.getTweetDate());
        em.remove(found);

        em.clear(); // clear cache.

        found = em.find(CouchDBEmbeddedUser.class, userId);
        Assert.assertNull(found);
        em.close();
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
        emf.close();
    }

}
