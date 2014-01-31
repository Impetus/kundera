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
package com.impetus.client.persistence;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.query.QueryImpl;

/**
 * Junit test case for NativeQuery support.
 * 
 * @author vivek.mishra
 * 
 */
public class NativeQueryTest
{
    // /** The schema. */
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
        CassandraCli.createKeySpace(schema);
        emf = getEntityManagerFactory();
    }

    /**
     * Test create native query.
     */
    @Test
    public void testCreateNativeQuery()
    {
        EntityManager em = emf.createEntityManager();
        String nativeSql = "Select * from Cassandra c";

        QueryImpl q = (QueryImpl) em.createNativeQuery(nativeSql, CassandraEntitySample.class);
        Assert.assertEquals(nativeSql, q.getJPAQuery());
    }

    /**
     * Test execute native create keyspace query.
     */
    @Test
    public void testExecutNativeQuery()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";

        EntityManager em = emf.createEntityManager();
        Query q = em.createNativeQuery(useNativeSql, CassandraEntitySample.class);
        q.executeUpdate();
        Assert.assertTrue(CassandraCli.keyspaceExist(schema));
        Assert.assertFalse(CassandraCli.keyspaceExist("invalidSchema"));
    }

    /**
     * Native queries should not leak connections. Pelops pool fails providing a
     * connection if we don't call {@link IPooledConnection#release()}
     */
    @Test
    public void testReleasesNativeQueryConnection()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";

        EntityManager em = emf.createEntityManager();

        // won't be able to loop if connections are leaked
        for (int i = 0; i < 30; i++)
        {
            Query q = em.createNativeQuery(useNativeSql, CassandraEntitySample.class);
            q.executeUpdate();
        }
    }

    /**
     * Test create insert column family query.
     */
    @Test
    public void testCreateInsertColumnFamilyQuery()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        EntityManager em = emf.createEntityManager();
        Query q = em.createNativeQuery(useNativeSql, CassandraEntitySample.class);
        // q.getResultList();
        q.executeUpdate();
        // create column family
        String colFamilySql = "CREATE COLUMNFAMILY users (key varchar PRIMARY KEY,full_name varchar, birth_date int,state varchar)";
        q = em.createNativeQuery(colFamilySql, CassandraEntitySample.class);
        // q.getResultList();
        q.executeUpdate();
        Assert.assertTrue(CassandraCli.columnFamilyExist("users", "test"));

        // Add indexes
        String idxSql = "CREATE INDEX ON users (birth_date)";
        q = em.createNativeQuery(idxSql, CassandraEntitySample.class);
        // q.getResultList();
        q.executeUpdate();
        idxSql = "CREATE INDEX ON users (state)";
        q = em.createNativeQuery(idxSql, CassandraEntitySample.class);
        // q.getResultList();
        q.executeUpdate();
        // insert users.
        String insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('bsanderson', 'Brandon Sanderson', 1975, 'UT')";
        q = em.createNativeQuery(insertSql, CassandraEntitySample.class);
        // q.getResultList();
        q.executeUpdate();
        // select key and state
        String selectSql = "SELECT key, state FROM users";

        q = em.createNativeQuery(selectSql, CassandraEntitySample.class);
        List<CassandraEntitySample> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("bsanderson", results.get(0).getKey());
        Assert.assertEquals("UT", results.get(0).getState());
        Assert.assertNull(results.get(0).getFull_name());

        // insert users.
        insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('prothfuss', 'Patrick Rothfuss', 1973, 'WI')";
        q = em.createNativeQuery(insertSql, CassandraEntitySample.class);
        q.getResultList();

        insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('htayler', 'Howard Tayler', 1968, 'UT')";
        q = em.createNativeQuery(insertSql, CassandraEntitySample.class);
        q.getResultList();

        // select all
        String selectAll = "SELECT * FROM users WHERE state='UT' AND birth_date > 1970 ALLOW FILTERING";
        q = em.createNativeQuery(selectAll, CassandraEntitySample.class);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("bsanderson", results.get(0).getKey());
        Assert.assertEquals("UT", results.get(0).getState());
        Assert.assertEquals("Brandon Sanderson", results.get(0).getFull_name());
        Assert.assertEquals(new Integer(1975), results.get(0).getBirth_date());

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
