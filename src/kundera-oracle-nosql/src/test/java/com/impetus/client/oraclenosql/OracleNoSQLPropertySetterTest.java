package com.impetus.client.oraclenosql;

/**
 * Copyright 2013 Impetus Infotech.
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

import com.impetus.client.oraclenosql.OracleNoSQLClient;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.KunderaMetadata;

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;

/**
 * Test case for {@link OracleNoSQLClient}
 * 
 * @author Chhavi Gangwal
 */
public class OracleNoSQLPropertySetterTest
{

    /** The Constant REDIS_PU. */
    private static final String PU = "twikvstore";

    /** The emf. */
    private EntityManagerFactory emf;

    EntityManager em;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(OracleNoSQLClient.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();

    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
    }

    @Test
    public void testProperties()
    {

        // Get properties from client
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        OracleNoSQLClient client = (OracleNoSQLClient) clients.get(PU);

        // Check default values
        Durability du = client.getDurability();
        Consistency consist = client.getConsistency();
        TimeUnit tu = client.getTimeUnit();
        Assert.assertNotNull(du);
        Assert.assertNotNull(consist);
        Assert.assertNotNull(tu);
        Assert.assertTrue(du instanceof Durability);

        // Set parameters into EM
        em.setProperty("write.timeout", 5);
        em.setProperty("durability", Durability.COMMIT_NO_SYNC);
        em.setProperty("time.unit", TimeUnit.HOURS);
        em.setProperty("consistency", Consistency.ABSOLUTE);
        em.setProperty(PersistenceProperties.KUNDERA_BATCH_SIZE, 5);

        Assert.assertEquals(5, client.getBatchSize());
        Assert.assertEquals(5, client.getTimeout());
        Assert.assertEquals(TimeUnit.HOURS, client.getTimeUnit());
        Assert.assertEquals(Consistency.ABSOLUTE, client.getConsistency());
        Assert.assertEquals(Durability.COMMIT_NO_SYNC, client.getDurability());

        em.clear();

        // Set parameters into EMas string
        em.setProperty("write.timeout", "10");
        em.setProperty(PersistenceProperties.KUNDERA_BATCH_SIZE, "10");
        em.setProperty("time.unit", "" + TimeUnit.HOURS);

        Assert.assertEquals(10, client.getTimeout());
        Assert.assertEquals(TimeUnit.HOURS, client.getTimeUnit());
        Assert.assertEquals(10, client.getBatchSize());
    }
}
