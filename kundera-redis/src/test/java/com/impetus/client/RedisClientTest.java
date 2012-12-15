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

package com.impetus.client;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.entities.PersonRedis;
import com.impetus.client.redis.RedisClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.graph.Node;

/**
 * Junit for {@link RedisClient}.
 * 
 * @author vivek.mishra
 */

public class RedisClientTest
{

    private static final String ROW_KEY = "1";

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redis_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisClientFactoryTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(REDIS_PU);
    }

    @Test
    public void testCRUD()
    {
        logger.info("On testInsert");
        EntityManager em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        RedisClient client = (RedisClient) clients.get(REDIS_PU);
        onInsert(client);
        onUpdate(client);
        onDelete(client);
        em.close();
    }

    /**
     * Assertions on delete.
     * 
     * @param client Redis client instance.
     */
    private void onUpdate(RedisClient client)
    {
        PersonRedis result = (PersonRedis) client.find(PersonRedis.class, ROW_KEY);
        Assert.assertNotNull(result);

        String updatedName = "Updated";
        result.setPersonName(updatedName);
        result.setAge(33);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getAge(), new Integer(33));
        Assert.assertEquals(result.getPersonName(), updatedName);
    }

    /**
     * Assertions on delete.
     * 
     * @param client Redis client instance.
     */
    private void onDelete(RedisClient client)
    {
        PersonRedis result = (PersonRedis) client.find(PersonRedis.class, ROW_KEY);
        Assert.assertNotNull(result);
        client.delete(result, ROW_KEY);
        result = (PersonRedis) client.find(PersonRedis.class, ROW_KEY);
        Assert.assertNull(result);
    }

    /**
     * Assertions on insert.
     * 
     * @param client Redis client instance.
     */
    private void onInsert(RedisClient client)
    {
        // RedisClient client = (RedisClient) clients.get(REDIS_PU);
        final String nodeId = "node1";
        final String originalName = "vivek";
        PersonRedis object = new PersonRedis();
        object.setAge(32);
        object.setPersonId(ROW_KEY);
        object.setPersonName(originalName);

        Node node = new Node(nodeId, object, null, object.getPersonId());
        client.persist(node);

        PersonRedis result = (PersonRedis) client.find(PersonRedis.class, ROW_KEY);

        Assert.assertNotNull(result);
        Assert.assertEquals(result.getPersonId(), object.getPersonId());
        Assert.assertEquals(result.getAge(), object.getAge());
        Assert.assertEquals(result.getPersonName(), object.getPersonName());

    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

}
