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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.junit.ContiPerfRule;
import org.databene.contiperf.report.CSVSummaryReportModule;
import org.databene.contiperf.report.HtmlReportModule;
import org.databene.contiperf.report.ReportModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.entities.PersonRedis;
import com.impetus.client.redis.RedisClient;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.lifecycle.states.TransientState;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;

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
    private static Logger logger = LoggerFactory.getLogger(RedisClientTest.class);

    @Rule
    public ContiPerfRule i = new ContiPerfRule(new ReportModule[] { new CSVSummaryReportModule(),
            new HtmlReportModule() });

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(REDIS_PU);
    }

    @Test
    @PerfTest(invocations = 10)
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

    @Test
    public void testCRUDWithBatch()
    {
        Map<String, String> batchProperty = new HashMap<String, String>(1);
        batchProperty.put(PersistenceProperties.KUNDERA_BATCH_SIZE, "5");
        emf = Persistence.createEntityManagerFactory(REDIS_PU, batchProperty);
        EntityManager em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        RedisClient client = (RedisClient) clients.get(REDIS_PU);
        Assert.assertEquals(5, ((Batcher) client).getBatchSize());

        final String originalName = "vivek";

        for (int i = 0; i < 9; i++)
        {
            PersonRedis object = new PersonRedis();
            object.setAge(32);
            object.setPersonId(ROW_KEY + i);
            object.setPersonName(originalName);
            em.persist(object);

            if (i >= 5)
            {
                PersonRedis result = (PersonRedis) client.find(PersonRedis.class, ROW_KEY + i);
                Assert.assertNull(result);
            }
            else if (i > 0 && i % 4 == 0)
            {
                PersonRedis result = (PersonRedis) client.find(PersonRedis.class, ROW_KEY + i);
                Assert.assertNotNull(result);
                Assert.assertEquals(result.getPersonId(), object.getPersonId());
                Assert.assertEquals(result.getAge(), object.getAge());
                Assert.assertEquals(result.getPersonName(), object.getPersonName());
            }
        }
        em.flush();
        em.clear();
        em.close();
        em = null;

        // emf.close();
        //
        // emf = null;
        // batchProperty.put(PersistenceProperties.KUNDERA_BATCH_SIZE, null);
        // emf = Persistence.createEntityManagerFactory(REDIS_PU,
        // batchProperty);

    }

    @Test
    public void testPersistJoinTableData()
    {
        final String schemaName = "redis";
        final String tableName = "redisjointable";
        final String joinColumn = "joincolumn";
        final String inverseJoinColumn = "inverseJoinColumnName";

        JoinTableData joinTableData = new JoinTableData(OPERATION.INSERT, "redis", "redisjointable", "joincolumn",
                "inverseJoinColumnName", null);

        UUID joinKey = UUID.randomUUID();

        Integer inverseJoinKey1 = new Integer(10);
        Double inverseJoinKey2 = new Double(12.23);
        Set inverseJoinKeys = new HashSet();
        inverseJoinKeys.add(inverseJoinKey1);
        inverseJoinKeys.add(inverseJoinKey2);

        joinTableData.addJoinTableRecord(joinKey, inverseJoinKeys);

        EntityManager em = emf.createEntityManager();
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        RedisClient client = (RedisClient) clients.get(REDIS_PU);
        client.persistJoinTable(joinTableData);

        List<String> columns = client.getColumnsById(schemaName, tableName, joinColumn, inverseJoinColumn, joinKey);

        Assert.assertNotNull(columns);
        Assert.assertEquals(true, !columns.isEmpty());
        Assert.assertEquals(2, columns.size());
        Assert.assertEquals(true, columns.contains(inverseJoinKey1.toString()));
        Assert.assertEquals(true, columns.contains(inverseJoinKey2.toString()));

        client.deleteByColumn(schemaName, tableName, inverseJoinColumn, inverseJoinKey1);
        client.deleteByColumn(schemaName, tableName, inverseJoinColumn, inverseJoinKey2);

        columns = client.getColumnsById(schemaName, tableName, joinColumn, inverseJoinColumn, joinKey);

        Assert.assertTrue(columns.isEmpty());

    }

    /**
     * Assertions on delete.
     * 
     * @param client
     *            Redis client instance.
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
     * @param client
     *            Redis client instance.
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
     * @param client
     *            Redis client instance.
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

        Node node = new Node(nodeId, PersonRedis.class, new TransientState(), null, ROW_KEY);
        node.setData(object);
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
        EntityManager em = emf.createEntityManager();

        // Delete by query.
        String deleteQuery = "Delete from PersonRedis p";
        Query query = em.createQuery(deleteQuery);
        query.executeUpdate();

        em.close();

        emf.close();
    }

}
