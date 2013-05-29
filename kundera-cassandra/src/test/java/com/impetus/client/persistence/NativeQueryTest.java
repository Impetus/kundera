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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.scale7.cassandra.pelops.pool.IThriftPool.IPooledConnection;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.pelops.PelopsClient;
import com.impetus.client.cassandra.thrift.ThriftClient;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.ClientFactoryConfiguraton;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
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
    }

    /**
     * Test create native query.
     */
    @Test
    public void testCreateNativeQuery()
    {
        EntityManagerFactoryImpl emf = getEntityManagerFactory();
        EntityManager em = emf.createEntityManager()/*
                                                     * new
                                                     * EntityManagerImpl(emf,
                                                     * PersistenceUnitTransactionType
                                                     * .RESOURCE_LOCAL,
                                                     * PersistenceContextType
                                                     * .EXTENDED)
                                                     */;
        String nativeSql = "Select * from Cassandra c";

        QueryImpl q = (QueryImpl) em.createNativeQuery(nativeSql, CassandraEntitySample.class);
        Assert.assertEquals(nativeSql, q.getJPAQuery());
        Assert.assertEquals(true, KunderaMetadata.INSTANCE.getApplicationMetadata().isNative(nativeSql));
    }

    /**
     * Test execute native create keyspace query.
     */
    @Test
    public void testExecutNativeQuery()
    {
        EntityManagerFactoryImpl emf = getEntityManagerFactory();
        // String nativeSql = "CREATE KEYSPACE " + schema
        // +
        // " with strategy_class = 'SimpleStrategy' and strategy_options:replication_factor=1";
        String useNativeSql = "USE " + "KunderaExamples";

        EntityManager em = emf.createEntityManager()/*
                                                     * new
                                                     * EntityManagerImpl(emf,
                                                     * PersistenceUnitTransactionType
                                                     * .RESOURCE_LOCAL,
                                                     * PersistenceContextType
                                                     * .EXTENDED)
                                                     */;
//        Map<String, Client> clientMap = (Map<String, Client>) em.getDelegate();
//        PelopsClient pc = (PelopsClient) clientMap.get("cassandra");
//        pc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        // Query q = em.createNativeQuery(nativeSql,
        // CassandraEntitySample.class);
        // // q.getResultList();
        // q.executeUpdate();
        Query q = em.createNativeQuery(useNativeSql, CassandraEntitySample.class);
        // q.getResultList();
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
        EntityManagerFactoryImpl emf = getEntityManagerFactory();
        // String nativeSql = "CREATE KEYSPACE "
        // + schema
        // +
        // " with strategy_class = 'SimpleStrategy' and strategy_options:replication_factor=1";
        // String useNativeSql = "USE test";
        String useNativeSql = "USE " + "KunderaExamples";

        EntityManager em = emf.createEntityManager()/*
                                                     * new
                                                     * EntityManagerImpl(emf,
                                                     * PersistenceUnitTransactionType
                                                     * .RESOURCE_LOCAL,
                                                     * PersistenceContextType
                                                     * .EXTENDED)
                                                     */;
//        Map<String, Client> clientMap = (Map<String, Client>) em.getDelegate();
//        PelopsClient pc = (PelopsClient) clientMap.get("cassandra");
//        pc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        // Query q = em.createNativeQuery(nativeSql,
        // CassandraEntitySample.class);
        // // q.getResultList();
        // q.executeUpdate();

        // won't be able to loop if connections are leaked
        for (int i = 0; i < 30; i++)
        {
            Query q = em.createNativeQuery(useNativeSql, CassandraEntitySample.class);
            // q.getResultList();
            q.executeUpdate();
        }
    }

    /**
     * Test create insert column family query.
     */
    @Test
    public void testCreateInsertColumnFamilyQuery()
    {
        // String nativeSql = "CREATE KEYSPACE " + schema
        // +
        // " with strategy_class = 'SimpleStrategy' and strategy_options:replication_factor=1";
        // String useNativeSql = "USE test";
        String useNativeSql = "USE " + "KunderaExamples";
        EntityManagerFactoryImpl emf = getEntityManagerFactory();
        EntityManager em = emf.createEntityManager()/*
                                                     * new
                                                     * EntityManagerImpl(emf,
                                                     * PersistenceUnitTransactionType
                                                     * .RESOURCE_LOCAL,
                                                     * PersistenceContextType
                                                     * .EXTENDED)
                                                     */;
//        Map<String, Client> clientMap = (Map<String, Client>) em.getDelegate();
//        PelopsClient pc = (PelopsClient) clientMap.get("cassandra");
//        pc.setCqlVersion(CassandraConstants.CQL_VERSION_3_0);
        // Query q = em.createNativeQuery(nativeSql,
        // CassandraEntitySample.class);
        // // q.getResultList();
        // q.executeUpdate();
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
        String selectAll = "SELECT * FROM users WHERE state='UT' AND birth_date > 1970";
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
        Map<String, Object> props = new HashMap<String, Object>();
        String persistenceUnit = "cassandra";
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY,
                "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaExamples");
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put("cassandra", puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(persistenceUnit);
        clazzToPu.put(CassandraEntitySample.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(CassandraEntitySample.class);
        TableProcessor processor = new TableProcessor(null);
        processor.process(CassandraEntitySample.class, m);
        m.setPersistenceUnit(persistenceUnit);
        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(CassandraEntitySample.class, m);
        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);
        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(persistenceUnit).getMappedSuperClassTypes());
        EntityManagerFactoryImpl emf = new EntityManagerFactoryImpl(persistenceUnit, props);
        String[] persistenceUnits = new String[] { persistenceUnit };
        new ClientFactoryConfiguraton(null, persistenceUnits).configure();
        return emf;
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
        // CassandraCli.dropKeySpace("KunderaExamples");
        CassandraCli.dropKeySpace(schema);
    }

}
