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

import java.lang.reflect.Field;
import java.util.HashMap;
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

import redis.clients.jedis.Jedis;
import com.impetus.client.redis.RedisClient;
import com.impetus.client.redis.RedisClientFactory;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.persistence.api.Batcher;

/**
 * The Class RedisClientFactoryTest. Junit for {@link RedisClientFactory}
 */
public class RedisClientFactoryTest
{

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redisClientFactoryTest_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisClientFactoryTest.class);

    /**
     * Setup.
     */
    @Before
    public void setup()
    {
        Map<String, String> properties = new HashMap<String, String>(1);
        properties.put("kundera.transaction.timeout", "20000");
        properties.put("kundera.pool.size.max.active", "10");
        properties.put("kundera.nodes", "localhost");
        properties.put("kundera.port", "6379");
        properties.put("kundera.batch.size", "5");
        emf = Persistence.createEntityManagerFactory(REDIS_PU, properties);
    }

    /**
     * Test connection.
     */
    @Test
    public void testConnection()
    {
        logger.info("On test connection");

        ClientFactory clientFactory = ClientResolver.getClientFactory(REDIS_PU);
        Assert.assertNotNull(clientFactory);
        Assert.assertEquals(RedisClientFactory.class, clientFactory.getClass());
        Field connectionField;
        try
        {
            String field_name = "connectionPoolOrConnection";
            
            connectionField = ((RedisClientFactory) clientFactory).getClass().getSuperclass()
                    .getDeclaredField(field_name);

            if (!connectionField.isAccessible())
            {
                connectionField.setAccessible(true);
            }

            Object connectionObj = connectionField.get(clientFactory);
           
            EntityManager em = emf.createEntityManager();
            Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
            RedisClient redisClient = (RedisClient) clients.get(REDIS_PU);
            Assert.assertEquals(5, ((Batcher) redisClient).getBatchSize());
            Field field=redisClient.getClass().getDeclaredField("connection");
            field.setAccessible(true);
            Jedis connection=(Jedis) field.get(redisClient);
            Assert.assertEquals(6379, connection.getClient().getPort());
            Assert.assertEquals("localhost", connection.getClient().getHost());
            Assert.assertEquals(20000, connection.getClient().getConnectionTimeout());
            
            Assert.assertNotNull(connectionObj);

        }
        catch (SecurityException e)
        {
            logger.error(e.getMessage());
            Assert.fail(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            logger.error(e.getMessage());
            Assert.fail(e.getMessage());
        }
        catch (IllegalArgumentException e)
        {
            logger.error(e.getMessage());
            Assert.fail(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            logger.error(e.getMessage());
            Assert.fail(e.getMessage());
        }
       
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        emf.close();
    }
}
