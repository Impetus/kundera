package com.impetus.client;

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

import com.impetus.client.entities.RedisCompoundKey;
import com.impetus.client.entities.RedisPrimeUser;

public class RedisCompositeKeyTest {

	/** The Constant REDIS_PU. */
	private static final String REDIS_PU = "redis_pu";

	/** The emf. */
	private EntityManagerFactory emf;

	/** The logger. */
	private static Logger logger = LoggerFactory
			.getLogger(RedisCompositeKeyTest.class);

	@Before
	public void setUp() throws Exception {
		emf = Persistence.createEntityManagerFactory(REDIS_PU);
	}

	 @Test
	public void testCRUD() {
		logger.info("On testCRUD");
		EntityManager em = emf.createEntityManager();
		final String userId = "1";
		final int tweetId = 12;
		final UUID timeLineId = UUID.randomUUID();
		final Date tweetDate = new Date();
		RedisCompoundKey compoundKey = new RedisCompoundKey(userId, tweetId,
				timeLineId);
		RedisPrimeUser user = new RedisPrimeUser(compoundKey);
		user.setTweetBody("My tweet");
		user.setTweetDate(tweetDate);
		em.persist(user);

		em.clear(); // clear cache.

		RedisPrimeUser found = em.find(RedisPrimeUser.class, compoundKey);
		Assert.assertNotNull(found);
		Assert.assertNotNull(found.getKey());
		Assert.assertEquals(userId, found.getKey().getUserId());
		Assert.assertEquals(tweetDate, found.getTweetDate());
		em.remove(found);

		em.clear(); // clear cache.

		found = em.find(RedisPrimeUser.class, compoundKey);
		Assert.assertNull(found);
	}

	@After
	public void tearDown() throws Exception {
		emf.close();
	}

}
