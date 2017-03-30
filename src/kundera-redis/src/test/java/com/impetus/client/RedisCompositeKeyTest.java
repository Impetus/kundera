/*******************************************************************************
 * * Copyright 2017 Impetus Infotech.
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
package com.impetus.client;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.entities.RedisCompoundKey;
import com.impetus.client.entities.RedisPrimeUser;

import junit.framework.Assert;

/**
 * The Class RedisCompositeKeyTest.
 */
public class RedisCompositeKeyTest
{

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redis_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up.
     *
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {

        emf = Persistence.createEntityManagerFactory(REDIS_PU);
        em = emf.createEntityManager();
    }

    /**
     * Test operations.
     *
     * @throws Exception
     *             the exception
     */
    @Test
    public void testOperations() throws Exception
    {
        testInsert();
        testQuery();
        testDelete();
    }

    /**
     * Test insert.
     *
     * @throws Exception
     *             the exception
     */
    public void testInsert() throws Exception
    {
        RedisCompoundKey compoundKey1 = new RedisCompoundKey("1", 101, UUID.randomUUID());
        RedisPrimeUser user1 = new RedisPrimeUser(compoundKey1);
        user1.setTweetBody("My tweet 1");
        user1.setTweetDate(new Date());
        em.persist(user1);

        RedisCompoundKey compoundKey2 = new RedisCompoundKey("2", 102, UUID.randomUUID());
        RedisPrimeUser user2 = new RedisPrimeUser(compoundKey2);
        user2.setTweetBody("My tweet 2");
        user2.setTweetDate(new Date());
        em.persist(user2);

        RedisCompoundKey compoundKey3 = new RedisCompoundKey("3", 103, UUID.randomUUID());
        RedisPrimeUser user3 = new RedisPrimeUser(compoundKey3);
        user3.setTweetBody("My tweet 3");
        user3.setTweetDate(new Date());
        em.persist(user3);

        em.clear();

        RedisPrimeUser user = em.find(RedisPrimeUser.class, compoundKey1);
        Assert.assertNotNull(user);
        Assert.assertNotNull(user.getKey());
        Assert.assertEquals("1", user.getKey().getUserId());
        Assert.assertEquals(101, user.getKey().getTweetId());
        Assert.assertEquals("My tweet 1", user.getTweetBody());

    }

    /**
     * Test query.
     *
     * @throws Exception
     *             the exception
     */
    public void testQuery() throws Exception
    {
        RedisPrimeUser user = null;
        List<RedisPrimeUser> users = em.createQuery("select u from RedisPrimeUser u").getResultList();
        Assert.assertEquals(3, users.size());

        users = em.createQuery("select u from RedisPrimeUser u where u.tweetBody='My tweet 2'").getResultList();
        Assert.assertEquals(1, users.size());
        Assert.assertNotNull(users.get(0));
        user = users.get(0);
        Assert.assertEquals("2", user.getKey().getUserId());
        Assert.assertEquals(102, user.getKey().getTweetId());
        Assert.assertEquals("My tweet 2", user.getTweetBody());
    }

    /**
     * Test delete.
     *
     * @throws Exception
     *             the exception
     */
    public void testDelete() throws Exception
    {
        String deleteQuery = "Delete from RedisPrimeUser p";
        em.createQuery(deleteQuery).executeUpdate();

        List<RedisPrimeUser> users = em.createQuery("select u from RedisPrimeUser u").getResultList();
        Assert.assertTrue(users.isEmpty());
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
        String deleteQuery = "Delete from RedisPrimeUser p";
        em.createQuery(deleteQuery).executeUpdate();

        if (em != null)
        {
            em.close();
        }
        if (emf != null)
        {
            emf.close();
        }
    }

}