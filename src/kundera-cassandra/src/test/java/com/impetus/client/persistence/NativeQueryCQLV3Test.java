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
package com.impetus.client.persistence;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.pelops.PelopsClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * <Prove description of functionality provided by this Type>
 * 
 * @author amresh.singh
 */
public class NativeQueryCQLV3Test
{

    private final String schema = "KunderaExamples";

    private EntityManagerFactoryImpl emf;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.dropKeySpace(schema);
        String nativeSql = "CREATE KEYSPACE \"" + schema
                + "\" with replication = {'class':'SimpleStrategy', 'replication_factor':1}";
        CassandraCli.getClient().execute_cql3_query(ByteBuffer.wrap(nativeSql.getBytes("UTF-8")), Compression.NONE,
                ConsistencyLevel.ONE);
        emf = getEntityManagerFactory();
    }

    /**
     * Test create insert column family query.
     */
    @Test
    public void testCreateInsertColumnFamilyQueryVersion3()
    {

        String useNativeSql = "USE \"" + schema + "\"";
        EntityManager em = emf.createEntityManager();

        Map<String, Client> clientMap = (Map<String, Client>) em.getDelegate();
        PelopsClient pc = (PelopsClient) clientMap.get("cassandra");
        pc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        Query q = em.createNativeQuery(useNativeSql, CassandraEntity.class);
        q.executeUpdate();
        // create column family
        String colFamilySql = "CREATE COLUMNFAMILY users (key varchar PRIMARY KEY,full_name varchar, birth_date int,state varchar)";
        q = em.createNativeQuery(colFamilySql, CassandraEntity.class);
        q.executeUpdate();
        Assert.assertTrue(CassandraCli.columnFamilyExist("users", "test"));

        // Add indexes
        String idxSql = "CREATE INDEX ON users (birth_date)";
        q = em.createNativeQuery(idxSql, CassandraEntity.class);
        q.executeUpdate();
        idxSql = "CREATE INDEX ON users (state)";
        q = em.createNativeQuery(idxSql, CassandraEntity.class);
        q.executeUpdate();
        // insert users.
        String insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('bsanderson', 'Brandon Sanderson', 1975, 'UT')";
        q = em.createNativeQuery(insertSql, CassandraEntity.class);
        q.executeUpdate();
        // select key and state
        String selectSql = "SELECT key, state FROM users";

        q = em.createNativeQuery(selectSql, CassandraEntity.class);
        List<CassandraEntity> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("bsanderson", results.get(0).getKey());
        Assert.assertEquals("UT", results.get(0).getState());
        Assert.assertNull(results.get(0).getFull_name());

        // insert users.
        insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('prothfuss', 'Patrick Rothfuss', 1973, 'WI')";
        q = em.createNativeQuery(insertSql, CassandraEntity.class);
        q.getResultList();

        insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('htayler', 'Howard Tayler', 1968, 'UT')";
        q = em.createNativeQuery(insertSql, CassandraEntity.class);
        q.getResultList();

        // select all
        String selectAll = "SELECT * FROM users WHERE state='UT' AND birth_date > 1970 Allow Filtering";
        q = em.createNativeQuery(selectAll, CassandraEntity.class);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("bsanderson", results.get(0).getKey());
        Assert.assertEquals("UT", results.get(0).getState());
        Assert.assertEquals("Brandon Sanderson", results.get(0).getFull_name());
        Assert.assertEquals(new Integer(1975), results.get(0).getBirth_date());
        emf.close();
    }

    /**
     * Gets the entity manager factory.
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory("cassandra");
    }

    @Test
    public void testCQLBatch()
    {
        String useNativeSql = "USE \"" + schema + "\"";
        String createColumnFamily = "CREATE TABLE CassandraBatchEntity ( user_name varchar PRIMARY KEY, password varchar, name varchar)";
        String batchOps = "BEGIN BATCH INSERT INTO CassandraBatchEntity (user_name, password, name) VALUES ('user2', 'ch@ngem3b', 'second user') UPDATE CassandraBatchEntity SET password = 'ps22dhds' WHERE user_name = 'user2' INSERT INTO CassandraBatchEntity (user_name, password) VALUES ('user3', 'ch@ngem3c') DELETE name FROM CassandraBatchEntity WHERE user_name = 'user2' INSERT INTO CassandraBatchEntity (user_name, password, name) VALUES ('user4', 'ch@ngem3c', 'Andrew') APPLY BATCH";

        EntityManager em = emf.createEntityManager();

        Map<String, Client> clientMap = (Map<String, Client>) em.getDelegate();
        PelopsClient pc = (PelopsClient) clientMap.get("cassandra");
        pc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);

        Query q = em.createNativeQuery(useNativeSql);
        // q.getResultList();
        q.executeUpdate();

        pc.setConsistencyLevel(ConsistencyLevel.QUORUM);
        q = em.createNativeQuery(createColumnFamily, CassandraBatchEntity.class);
        // q.getResultList();
        q.executeUpdate();

        pc.setConsistencyLevel(ConsistencyLevel.QUORUM);
        q = em.createNativeQuery(batchOps, CassandraBatchEntity.class);
        // q.getResultList();
        q.executeUpdate();

        q = em.createNativeQuery("select * from CassandraBatchEntity", CassandraBatchEntity.class);
        List<CassandraBatchEntity> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

        // Multiple table batch processing.

        createColumnFamily = "create table test1 (id timeuuid primary key, url text, userid uuid, datetime timestamp, linkcounts int)";
        em.createNativeQuery(createColumnFamily, CassandraBatchEntity.class).executeUpdate();
        createColumnFamily = "create table test2 (key text primary key, count int)";
        em.createNativeQuery(createColumnFamily, CassandraBatchEntity.class).executeUpdate();
        batchOps = "BEGIN BATCH INSERT INTO test1(id, url) VALUES (64907b40-29a1-11e2-93fa-90b11c71b811,'w') INSERT INTO test2(key, count) VALUES ('key1',12) APPLY BATCH";
        em.createNativeQuery(batchOps, CassandraBatchEntity.class).executeUpdate();
        emf.close();
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
        CassandraCli.dropKeySpace(schema);
    }

}
