/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.crud.countercolumns;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.persistence.api.Batcher;

/**
 * The Class CountersBatchTest.
 * 
 * @author: karthikp.manchala
 */
public class CountersCQL3BatchTest
{

    /** The Constant COUNTERS_PU. */
    private static final String COUNTERS_PU = "CassandraCounterTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The Constant RUN_IN_EMBEDDED_MODE. */
    private static final boolean RUN_IN_EMBEDDED_MODE = true;

    /** The keyspace. */
    private String keyspace = "KunderaCounterColumn";

    /** The property map. */
    protected Map propertyMap = new HashMap();

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            startServer();
        }

        if (propertyMap.isEmpty())
        {
            propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
            propertyMap.put(PersistenceProperties.KUNDERA_BATCH_SIZE, "5");
        }
        emf = Persistence.createEntityManagerFactory(COUNTERS_PU, propertyMap);
    }

    /**
     * Counters batch test.
     */
    @Test
    public void countersBatchTest()
    {
        EntityManager em = emf.createEntityManager();
        int count = 1;
        List<Counters> rows = prepareData(10);
        for (Counters entity : rows)
        {
            em.persist(entity);

            int BATCH_SIZE = 5;
            if (count < BATCH_SIZE)
            {
                Assert.assertNull(em.find(Counters.class, entity.getId()));
            }
            else if (count == BATCH_SIZE)
            {
                Map<String, Client> clients = (Map<String, Client>) em.getDelegate();

                Batcher client = (Batcher) clients.get(COUNTERS_PU);
                Assert.assertEquals(5, client.getBatchSize());
                em.clear();
                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    // assert on each batch size record
                    Assert.assertNotNull(em.find(Counters.class, rows.get(i).getId()));
                }
                count = 0;
            }
            count++;
        }
        em.close();
    }

    /**
     * Prepare data.
     * 
     * @param n
     *            the n
     * @return the list
     */
    private List<Counters> prepareData(int n)
    {
        List<Counters> rows = new ArrayList<Counters>();
        for (int i = 0; i < n; i++)
        {
            Counters counter = new Counters();
            counter.setCounter(1);
            counter.setId(i + "");
            rows.add(counter);
        }
        return rows;
    }

    /**
     * Start server.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    private void startServer() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        CassandraCli.cassandraSetUp();
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
        if (CassandraCli.client != null)
        {
            CassandraCli.dropKeySpace(keyspace);
        }
    }
}
