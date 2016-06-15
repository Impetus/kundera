/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.cassandra.query;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.metamodel.Metamodel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.crud.PersonCassandra;
import com.impetus.client.crud.compositeType.CassandraCompoundKey;
import com.impetus.client.crud.compositeType.CassandraPrimeUser;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityManagerImpl;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryParser;

/**
 * @author Kuldeep.Mishra
 * 
 */
public class CassQueryTest
{
    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(CassQueryTest.class);

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
    }

    @After
    public void tearDown() throws Exception
    {

    }

    /**
     * Test method for
     * {@link com.impetus.client.cassandra.query.CassQuery#onQueryOverCQL3(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client, com.impetus.kundera.metadata.model.MetamodelImpl, java.util.List)}
     * .
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testOnQueryOverCQL3ForSimpleEntity() throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException
    {
        CassandraCli.createKeySpace("KunderaExamples");
        String pu = "genericCassandraTest";
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(pu);
        EntityManager em = emf.createEntityManager();

        // Simple Query.
        String queryString = "Select p from PersonCassandra p WHERE p.personId = 'kk'";

        String cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf), emf, em, pu, PersonCassandra.class,
                200);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("SELECT * FROM \"PERSONCASSANDRA\" WHERE \"personId\" = 'kk' LIMIT 200", cqlQuery);

        // In Query.
        queryString = "Select p from PersonCassandra p WHERE p.personId in ('kk', 'sk','pk')";

        cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf), emf, em, pu, PersonCassandra.class, 400);

        Assert.assertNotNull(cqlQuery);

//        "SELECT * FROM "PERSONCASSANDRA" WHERE "personId" IN ('(''kk''', 'sk', '''pk') LIMIT 400";
        Assert.assertEquals("SELECT * FROM \"PERSONCASSANDRA\" WHERE \"personId\" IN ('kk', 'sk', 'pk') LIMIT 400", cqlQuery);

        // In Query set Paramater.
        queryString = "Select p from PersonCassandra p WHERE p.personId in :nameList";

        List<String> nameList = new ArrayList<String>();
        nameList.add("kk");
        nameList.add("dk");
        nameList.add("sk");

        KunderaQuery kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("nameList", nameList);

        cqlQuery = parseAndCreateCqlQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, Integer.MAX_VALUE);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("SELECT * FROM \"PERSONCASSANDRA\" WHERE \"personId\" IN ('kk', 'dk', 'sk' ) ", cqlQuery);

        // In Query set Paramater with and clause.
        queryString = "Select p from PersonCassandra p WHERE p.personId in :nameList and p.age = 10";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("nameList", nameList);

        cqlQuery = parseAndCreateCqlQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, Integer.MAX_VALUE);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"personId\" IN ('kk', 'dk', 'sk' )  AND \"AGE\" = 10  ALLOW FILTERING",
                cqlQuery);

        // In Query set Paramater with gt clause.
        queryString = "Select p from PersonCassandra p WHERE p.personId in :nameList and p.age > 10";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("nameList", nameList);

        cqlQuery = parseAndCreateCqlQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, 5);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"personId\" IN ('kk', 'dk', 'sk' )  AND \"AGE\" > 10 LIMIT 5  ALLOW FILTERING",
                cqlQuery);

        // In Query over non pk field set Paramater with gt clause.
        queryString = "Select p from PersonCassandra p WHERE p.age in :ageList and p.personName = 'vivek'";

        List<Integer> ageList = new ArrayList<Integer>();
        ageList.add(10);
        ageList.add(20);
        ageList.add(30);

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("ageList", ageList);

        cqlQuery = parseAndCreateCqlQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, 100);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"AGE\" IN (10, 20, 30 )  AND \"PERSON_NAME\" = 'vivek' LIMIT 100  ALLOW FILTERING",
                cqlQuery);

        em.close();
        emf.close();

        CassandraCli.dropKeySpace("KunderaExamples");
    }

    /**
     * Test method for
     * {@link com.impetus.client.cassandra.query.CassQuery#onQueryOverCQL3(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client, com.impetus.kundera.metadata.model.MetamodelImpl, java.util.List)}
     * .
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testOnQueryOverCQL3ForEmbeddedEntity() throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException
    {
        CassandraCli.createKeySpace("CompositeCassandra");
        String pu = "composite_pu";
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(pu);
        EntityManager em = emf.createEntityManager();

        // Simple Query.
        String queryString = "Select u from CassandraPrimeUser u WHERE u.name = 'kk'";

        String cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf), emf, em, pu,
                CassandraPrimeUser.class, 100);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("SELECT * FROM \"CompositeUser\" WHERE \"name\" = 'kk' LIMIT 100  ALLOW FILTERING", cqlQuery);

        // In Query.
        queryString = "Select u from CassandraPrimeUser u WHERE u.key.userId IN ('kk','dk','sk') ORDER BY u.key.tweetId ASC";

        cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf), emf, em, pu, CassandraPrimeUser.class, 400);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "SELECT * FROM \"CompositeUser\" WHERE \"userId\" IN ('kk', 'dk', 'sk')  ORDER BY \"tweetId\" ASC  LIMIT 400  ALLOW FILTERING",
                cqlQuery);

        // In Query set Paramater.
        queryString = "Select u from CassandraPrimeUser u WHERE u.key.userId IN :userIdList ORDER BY u.key.tweetId ASC";

        List<String> userIdList = new ArrayList<String>();
        userIdList.add("kk");
        userIdList.add("dk");
        userIdList.add("sk");

        KunderaQuery kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("userIdList", userIdList);

        cqlQuery = parseAndCreateCqlQuery(kunderaQuery, emf, em, pu, CassandraPrimeUser.class, Integer.MAX_VALUE);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "SELECT * FROM \"CompositeUser\" WHERE \"userId\" IN ('kk', 'dk', 'sk' )   ORDER BY \"tweetId\" ASC   ALLOW FILTERING",
                cqlQuery);

        // In Query set Paramater with and clause.
        queryString = "Select u from CassandraPrimeUser u WHERE u.key.userId IN :userIdList and p.name = 'kuldeep' ORDER BY u.key.tweetId ASC";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("userIdList", userIdList);

        cqlQuery = parseAndCreateCqlQuery(kunderaQuery, emf, em, pu, CassandraPrimeUser.class, 100);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "SELECT * FROM \"CompositeUser\" WHERE \"userId\" IN ('kk', 'dk', 'sk' )  AND \"name\" = 'kuldeep'  ORDER BY \"tweetId\" ASC  LIMIT 100  ALLOW FILTERING",
                cqlQuery);

        // In Query set Paramater with gt clause.
        queryString = "Select u from CassandraPrimeUser u WHERE u.key IN :keyList";

        List<CassandraCompoundKey> personIdList = new ArrayList<CassandraCompoundKey>();
        UUID timeLineId1 = UUID.randomUUID();
        personIdList.add(new CassandraCompoundKey("kk", 1, timeLineId1));
        personIdList.add(new CassandraCompoundKey("vm", 2, timeLineId1));
        personIdList.add(new CassandraCompoundKey("vs", 3, timeLineId1));

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("keyList", personIdList);

        cqlQuery = parseAndCreateCqlQuery(kunderaQuery, emf, em, pu, CassandraPrimeUser.class, 100);

        Assert.assertNotNull(cqlQuery);
        Assert.assertTrue(cqlQuery.contains("SELECT * FROM \"CompositeUser\" WHERE "));
        Assert.assertTrue(cqlQuery.contains("\"timeLineId\" IN (" + timeLineId1 + ", " + timeLineId1 + ", "
                + timeLineId1 + " )"));
        Assert.assertTrue(cqlQuery.contains("\"userId\" IN ('kk', 'vm', 'vs' )"));
        Assert.assertTrue(cqlQuery.contains("\"tweetId\" IN (1, 2, 3 )"));
        Assert.assertTrue(cqlQuery.contains(" LIMIT 100"));
        Assert.assertTrue(cqlQuery.contains(" AND "));
        Assert.assertTrue(cqlQuery.indexOf(" AND ") > 0);

        // In Query over non pk field set Paramater with gt clause.
        queryString = "Select u from CassandraPrimeUser u WHERE u.key IN :keyList and u.name = 'vivek'";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("keyList", personIdList);

        cqlQuery = parseAndCreateCqlQuery(kunderaQuery, emf, em, pu, CassandraPrimeUser.class, 100);

        Assert.assertNotNull(cqlQuery);
        Assert.assertTrue(cqlQuery.contains("SELECT * FROM \"CompositeUser\" WHERE "));
        Assert.assertTrue(cqlQuery.contains("\"timeLineId\" IN (" + timeLineId1 + ", " + timeLineId1 + ", "
                + timeLineId1 + " )"));
        Assert.assertTrue(cqlQuery.contains("\"userId\" IN ('kk', 'vm', 'vs' )"));
        Assert.assertTrue(cqlQuery.contains("\"tweetId\" IN (1, 2, 3 )"));
        Assert.assertTrue(cqlQuery.contains(" LIMIT 100"));
        Assert.assertTrue(cqlQuery.contains(" AND "));
        Assert.assertTrue(cqlQuery.contains("AND \"name\" = 'vivek'"));
        Assert.assertTrue(cqlQuery.indexOf(" AND ") > 0);

        em.close();
        emf.close();

        CassandraCli.dropKeySpace("CompositeCassandra");
    }

    /**
     * Test method for
     * {@link com.impetus.client.cassandra.query.CassQuery#onQueryOverCQL3(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client, com.impetus.kundera.metadata.model.MetamodelImpl, java.util.List)}
     * .
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testUpdateDeleteQuerySimpleEntity() throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException
    {
        CassandraCli.createKeySpace("KunderaExamples");
        String pu = "genericCassandraTest";
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(pu);
        EntityManager em = emf.createEntityManager();

        // Simple Query.
        String queryString = "Update PersonCassandra p SET p.personName = 'Kuldeep' WHERE p.personId = '1'";

        String cqlQuery = parseAndCreateUpdateQuery(getQueryObject(queryString, emf), emf, em, pu,
                PersonCassandra.class, 200);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("UPDATE \"PERSONCASSANDRA\" SET \"PERSON_NAME\"='Kuldeep' WHERE \"personId\" = '1'", cqlQuery);

        // In Query.
        queryString = "Update PersonCassandra p SET p.personName = 'Kuldeep' WHERE p.personId IN ('1', '2', '3')";

        cqlQuery = parseAndCreateUpdateQuery(getQueryObject(queryString, emf), emf, em, pu, PersonCassandra.class, 400);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "UPDATE \"PERSONCASSANDRA\" SET \"PERSON_NAME\"='Kuldeep' WHERE \"personId\" IN ('1', '2', '3')", cqlQuery);

        // In Query set Paramater.
        queryString = "Update PersonCassandra p SET p.personName = 'Kuldeep' WHERE p.personId IN :idList";

        List<String> id = new ArrayList<String>();
        id.add("1");
        id.add("2");
        id.add("3");

        KunderaQuery kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("idList", id);

        cqlQuery = parseAndCreateUpdateQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, Integer.MAX_VALUE);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "UPDATE \"PERSONCASSANDRA\" SET \"PERSON_NAME\"='Kuldeep' WHERE \"personId\" IN ('1', '2', '3' ) ", cqlQuery);

        // In Query set Paramater with and clause.
        queryString = "Update PersonCassandra p SET p.personName = 'Kuldeep' WHERE p.personId in :nameList and p.age = 10";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("nameList", id);

        cqlQuery = parseAndCreateUpdateQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, Integer.MAX_VALUE);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "UPDATE \"PERSONCASSANDRA\" SET \"PERSON_NAME\"='Kuldeep' WHERE \"personId\" IN ('1', '2', '3' )  AND \"AGE\" = 10",
                cqlQuery);

        // In Query.
        queryString = "Delete from PersonCassandra p WHERE p.personId = '1'";

        cqlQuery = parseAndCreateDeleteQuery(getQueryObject(queryString, emf), emf, em, pu, PersonCassandra.class, 400);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("DELETE FROM \"PERSONCASSANDRA\" WHERE \"personId\" = '1'", cqlQuery);

        // In Query.
        queryString = "Delete from PersonCassandra p WHERE p.personId IN ('1', '2', '3')";

        cqlQuery = parseAndCreateDeleteQuery(getQueryObject(queryString, emf), emf, em, pu, PersonCassandra.class, 400);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("DELETE FROM \"PERSONCASSANDRA\" WHERE \"personId\" IN ('1', '2', '3')", cqlQuery);

        // In Query set Paramater.
        queryString = "Delete from PersonCassandra p WHERE p.personId IN :idList";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("idList", id);

        cqlQuery = parseAndCreateDeleteQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, Integer.MAX_VALUE);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("DELETE FROM \"PERSONCASSANDRA\" WHERE \"personId\" IN ('1', '2', '3' ) ", cqlQuery);

        // In Query over non pk field set Paramater with gt clause.
        queryString = "Delete from PersonCassandra p WHERE p.personId in :idList and p.age = 25";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("idList", id);

        cqlQuery = parseAndCreateDeleteQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, 100);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("DELETE FROM \"PERSONCASSANDRA\" WHERE \"personId\" IN ('1', '2', '3' )  AND \"AGE\" = 25",
                cqlQuery);

        em.close();
        emf.close();

        CassandraCli.dropKeySpace("KunderaExamples");
    }


    /**
     * Test method for
     * {@link com.impetus.client.cassandra.query.CassQuery#onQueryOverCQL3(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client, com.impetus.kundera.metadata.model.MetamodelImpl, java.util.List)}
     * .
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testUpdateQueryWithTTLSimpleEntity() throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException
    {
        CassandraCli.createKeySpace("KunderaExamples");
        String pu = "genericCassandraTest";
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(pu);
        EntityManager em = emf.createEntityManager();

        // Simple Query.
        String queryString = "Update PersonCassandra p SET p.personName = 'Kuldeep' WHERE p.personId = '1'";

        String cqlQuery = parseAndCreateUpdateQuery(getQueryObject(queryString, emf), emf, em, pu,
                PersonCassandra.class, 200,100);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("UPDATE \"PERSONCASSANDRA\"  USING TTL 100 SET \"PERSON_NAME\"='Kuldeep' WHERE \"personId\" = '1'", cqlQuery);

        // In Query.
        queryString = "Update PersonCassandra p SET p.personName = 'Kuldeep' WHERE p.personId IN ('1', '2', '3')";

        cqlQuery = parseAndCreateUpdateQuery(getQueryObject(queryString, emf), emf, em, pu, PersonCassandra.class, 400, 200);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "UPDATE \"PERSONCASSANDRA\"  USING TTL 200 SET \"PERSON_NAME\"='Kuldeep' WHERE \"personId\" IN ('1', '2', '3')", cqlQuery);

        // In Query set Paramater.
        queryString = "Update PersonCassandra p SET p.personName = 'Kuldeep' WHERE p.personId IN :idList";

        List<String> id = new ArrayList<String>();
        id.add("1");
        id.add("2");
        id.add("3");

        KunderaQuery kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("idList", id);

        cqlQuery = parseAndCreateUpdateQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, Integer.MAX_VALUE, 1000);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "UPDATE \"PERSONCASSANDRA\"  USING TTL 1000 SET \"PERSON_NAME\"='Kuldeep' WHERE \"personId\" IN ('1', '2', '3' ) ", cqlQuery);

        // In Query set Paramater with and clause.
        queryString = "Update PersonCassandra p SET p.personName = 'Kuldeep' WHERE p.personId in :nameList and p.age = 10";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("nameList", id);

        cqlQuery = parseAndCreateUpdateQuery(kunderaQuery, emf, em, pu, PersonCassandra.class, Integer.MAX_VALUE);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals(
                "UPDATE \"PERSONCASSANDRA\" SET \"PERSON_NAME\"='Kuldeep' WHERE \"personId\" IN ('1', '2', '3' )  AND \"AGE\" = 10",
                cqlQuery);

        em.close();
        emf.close();

        CassandraCli.dropKeySpace("KunderaExamples");
    }

    /**
     * Test method for
     * {@link com.impetus.client.cassandra.query.CassQuery#onQueryOverCQL3(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client, com.impetus.kundera.metadata.model.MetamodelImpl, java.util.List)}
     * .
     * 
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testUpdateDeleteQueryEmbeddedEntity() throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException
    {
        CassandraCli.createKeySpace("CompositeCassandra");
        String pu = "composite_pu";
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(pu);
        EntityManager em = emf.createEntityManager();

        // Simple Query.
        String queryString = "Update CassandraPrimeUser u SET u.name = 'Kuldeep' WHERE u.name = 'kk'";

        String cqlQuery = parseAndCreateUpdateQuery(getQueryObject(queryString, emf), emf, em, pu,
                CassandraPrimeUser.class, 100);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("UPDATE \"CompositeUser\" SET \"name\"='Kuldeep' WHERE \"name\" = 'kk'", cqlQuery);

        // In Query.
        queryString = "Update CassandraPrimeUser u SET u.name = 'Kuldeep' WHERE u.key.userId IN ('kk','dk','sk')";

        cqlQuery = parseAndCreateUpdateQuery(getQueryObject(queryString, emf), emf, em, pu, CassandraPrimeUser.class,
                400);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("UPDATE \"CompositeUser\" SET \"name\"='Kuldeep' WHERE \"userId\" IN ('kk', 'dk', 'sk')",
                cqlQuery);

        // In Query set Paramater.
        queryString = "Update CassandraPrimeUser u SET u.name = 'Kuldeep' WHERE u.key.userId IN :userIdList";

        List<String> userIdList = new ArrayList<String>();
        userIdList.add("kk");
        userIdList.add("dk");
        userIdList.add("sk");

        KunderaQuery kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("userIdList", userIdList);

        cqlQuery = parseAndCreateUpdateQuery(kunderaQuery, emf, em, pu, CassandraPrimeUser.class, Integer.MAX_VALUE);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("UPDATE \"CompositeUser\" SET \"name\"='Kuldeep' WHERE \"userId\" IN ('kk', 'dk', 'sk' ) ",
                cqlQuery);

        // Simple Query.
        queryString = "Delete from CassandraPrimeUser u WHERE u.name = 'kk'";

        cqlQuery = parseAndCreateDeleteQuery(getQueryObject(queryString, emf), emf, em, pu, CassandraPrimeUser.class,
                100);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("DELETE FROM \"CompositeUser\" WHERE \"name\" = 'kk'", cqlQuery);

        // In Query.
        queryString = "Delete from CassandraPrimeUser u WHERE u.key.userId IN ('kk', 'dk', 'sk' )";

        cqlQuery = parseAndCreateDeleteQuery(getQueryObject(queryString, emf), emf, em, pu, CassandraPrimeUser.class,
                400);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("DELETE FROM \"CompositeUser\" WHERE \"userId\" IN ('kk', 'dk', 'sk')", cqlQuery);

        // In Query set Paramater with and clause.
        queryString = "Delete from CassandraPrimeUser u WHERE u.key.userId IN :userIdList";

        kunderaQuery = getQueryObject(queryString, emf);
        kunderaQuery.setParameter("userIdList", userIdList);

        cqlQuery = parseAndCreateDeleteQuery(kunderaQuery, emf, em, pu, CassandraPrimeUser.class, 100);

        Assert.assertNotNull(cqlQuery);

        Assert.assertEquals("DELETE FROM \"CompositeUser\" WHERE \"userId\" IN ('kk', 'dk', 'sk' ) ", cqlQuery);

        em.close();
        emf.close();

        CassandraCli.dropKeySpace("CompositeCassandra");
    }

    /**
     * 
     * @param kunderaQuery
     * @return
     */
    private String parseAndCreateCqlQuery(KunderaQuery kunderaQuery, EntityManagerFactory emf, EntityManager em,
            String puName, Class entityClass, Integer maxResult)
    {
        Method getpd = null;
        try
        {
            getpd = EntityManagerImpl.class.getDeclaredMethod("getPersistenceDelegator");
        }
        catch (Exception e)
        {
            log.error("Error during execution, Caused by : ",e.getMessage());
        }
        
        getpd.setAccessible(true);
        PersistenceDelegator pd = getPersistenceDelegator(em, getpd);

        KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();

        CassQuery query = new CassQuery(kunderaQuery, pd, kunderaMetadata);
        query.setMaxResults(maxResult);

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        Metamodel metaModel = KunderaMetadataManager.getMetamodel(kunderaMetadata, puName);

        Client<CassQuery> client = pd.getClient(metadata);

        String cqlQuery = query.onQueryOverCQL3(metadata, client, (MetamodelImpl) metaModel,
                metadata.getRelationNames());
        return cqlQuery;
    }

    private PersistenceDelegator getPersistenceDelegator(EntityManager em, Method getpd)
    {
        PersistenceDelegator pd = null;
        try
        {
            pd = (PersistenceDelegator) getpd.invoke(em);
        }
        catch (Exception e)
        {
            log.error("Error during execution, Caused by : ",e.getMessage());
        }
        return pd;
    }

    /**
     * Parse and create update query.
     * 
     * @param kunderaQuery  kundera query.
     * @param emf  entity manager factory.
     * @param em   entity manager
     * @param puName     persistence unit name.
     * @param entityClass  entity class. 
     * @param maxResult max result.
     * 
     * @return parsed query.
     */
    private String parseAndCreateUpdateQuery(KunderaQuery kunderaQuery, EntityManagerFactory emf, EntityManager em,
            String puName, Class entityClass, Integer maxResult)
    {
        return parseAndCreateUpdateQuery(kunderaQuery, emf, em, puName, entityClass, maxResult, null);
    }
    /**
     * 
     * @param kunderaQuery
     * @return
     */
    private String parseAndCreateUpdateQuery(KunderaQuery kunderaQuery, EntityManagerFactory emf, EntityManager em,
            String puName, Class entityClass, Integer maxResult, Integer ttl)
    {
        Method getpd = null;
        try
        {
            getpd = EntityManagerImpl.class.getDeclaredMethod("getPersistenceDelegator");
        }
        catch (SecurityException e)
        {
        	log.warn(e.getMessage());
        }
        catch (NoSuchMethodException e)
        {
        	log.warn(e.getMessage());
        }
        getpd.setAccessible(true);
//        PersistenceDelegator pd = null;
        PersistenceDelegator pd = getPersistenceDelegator(em, getpd);
        KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();

        CassQuery query = new CassQuery(kunderaQuery, pd, kunderaMetadata);
        query.setMaxResults(maxResult);
        if(ttl != null)
        {
            query.applyTTL(ttl);
        }
        
        String cqlQuery = query.createUpdateQuery(kunderaQuery);
        return cqlQuery;
    }

    /**
     * 
     * @param kunderaQuery
     * @return
     */
    private String parseAndCreateDeleteQuery(KunderaQuery kunderaQuery, EntityManagerFactory emf, EntityManager em,
            String puName, Class entityClass, Integer maxResult)
    {
        Method getpd = null;
        try
        {
            getpd = EntityManagerImpl.class.getDeclaredMethod("getPersistenceDelegator");
        }
        catch (SecurityException e)
        {
        	log.warn(e.getMessage());
        }
        catch (NoSuchMethodException e)
        {
        	log.warn(e.getMessage());
        }
        getpd.setAccessible(true);
        PersistenceDelegator pd = null;
        try
        {
            pd = (PersistenceDelegator) getpd.invoke(em);
        }
        catch (IllegalArgumentException e)
        {
        	log.warn(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
        	log.warn(e.getMessage());
        }
        catch (InvocationTargetException e)
        {
        	log.warn(e.getMessage());
        }

        KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();

        CassQuery query = new CassQuery(kunderaQuery, pd, kunderaMetadata);
        query.setMaxResults(maxResult);

        String cqlQuery = query.createDeleteQuery(kunderaQuery);
        return cqlQuery;
    }

    /**
     * 
     * @param queryString
     * @return
     */
    private KunderaQuery getQueryObject(String queryString, EntityManagerFactory emf)
    {
        Method getpostParsingInit = null;
        try
        {
            getpostParsingInit = KunderaQuery.class.getDeclaredMethod("postParsingInit");
        }
        catch (SecurityException e)
        {
        	log.warn(e.getMessage());
        }
        catch (NoSuchMethodException e)
        {
        	log.warn(e.getMessage());
        }
        getpostParsingInit.setAccessible(true);

        KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
        KunderaQuery kunderaQuery = new KunderaQuery(queryString, kunderaMetadata);
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
        queryParser.parse();
        try
        {
            getpostParsingInit.invoke(kunderaQuery);
        }
        catch (IllegalArgumentException e)
        {
        	log.warn(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
        	log.warn(e.getMessage());
        }
        catch (InvocationTargetException e)
        {
        	log.warn(e.getMessage());
        }

        return kunderaQuery;
    }
}
