/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.neo4j;


import java.lang.reflect.Field;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.loader.ClientFactory;

/**
 * Test case for {@link Neo4JClientFactory} 
 * @author amresh.singh
 */
public class Neo4JClientFactoryTest
{

    /** The Constant REDIS_PU. */
    private static final String persistenceUnit = "imdb";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(Neo4JClientFactoryTest.class);

    /**
     * Setup.
     */
    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
    }

    /**
     * Test connection.
     */
    @Test
    public void testConnection()
    {
        logger.info("On test connection");

        ClientFactory clientFactory = ClientResolver.getClientFactory(persistenceUnit);
        Assert.assertNotNull(clientFactory);
        Assert.assertEquals(Neo4JClientFactory.class, clientFactory.getClass());
        Field connectionField;
        try
        {
            String field_name = "connectionPoolOrConnection";
            connectionField = ((Neo4JClientFactory) clientFactory).getClass().getSuperclass()
                    .getDeclaredField(field_name);

            if (!connectionField.isAccessible())
            {
                connectionField.setAccessible(true);
            }

            Object connectionObj = connectionField.get(clientFactory);

            Assert.assertNotNull(connectionObj);
            Assert.assertTrue(connectionObj instanceof EmbeddedGraphDatabase);

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
