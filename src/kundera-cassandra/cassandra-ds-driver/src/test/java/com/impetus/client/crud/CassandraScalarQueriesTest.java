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
package com.impetus.client.crud;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.crud.PersonCassandra.Day;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * The Class CassandraScalarQueriesTest.
 * 
 * @author: karthikp.manchala
 */
public class CassandraScalarQueriesTest
{

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.dropKeySpace("KunderaExamples");
        HashMap propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("dsscalar_entity", propertyMap);
        EntityManager em = emf.createEntityManager();

        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
    }

    /**
     * Test scalar query.
     */
    @Test
    public void testSelectQueries()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("dsscalar");
        EntityManager entityManager = emf.createEntityManager();
        String qry = "Select \"personId\", \"PERSON_NAME\" from \"PERSON\" where \"personId\" = '1'";

        Query q = entityManager.createNativeQuery(qry);
        List<Object[]> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());

        Assert.assertEquals("1", persons.get(0)[0]);
        Assert.assertEquals("karthik", persons.get(0)[1]);

        qry = "Select \"personId\", \"PERSON_NAME\", \"MONTH_ENUM\", \"AGE\" from \"PERSON\" where \"personId\" = '1'";
        q = entityManager.createNativeQuery(qry);

        persons = q.getResultList();

        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());

        Assert.assertEquals("1", persons.get(0)[0]);
        Assert.assertEquals("karthik", persons.get(0)[1]);
        Assert.assertEquals("MAY", persons.get(0)[2]);
        Assert.assertEquals(10, persons.get(0)[3]);

        qry = "Select \"personId\", \"PERSON_NAME\", \"MONTH_ENUM\", \"AGE\" from \"PERSON\"";
        q = entityManager.createNativeQuery(qry);

        persons = q.getResultList();

        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(3, persons.size());

        Assert.assertEquals("1", persons.get(0)[0]);
        Assert.assertEquals("karthik", persons.get(0)[1]);
        Assert.assertEquals("MAY", persons.get(0)[2]);
        Assert.assertEquals(10, persons.get(0)[3]);

        entityManager.close();
        emf.close();
    }

    /**
     * Test on column family.
     */
    @Test
    public void testCreateAndUpdateQueries()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("dsscalar");
        EntityManager entityManager = emf.createEntityManager();
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        Query q = entityManager.createNativeQuery(useNativeSql);
        q.executeUpdate();
        // create column family
        String colFamilySql = "CREATE COLUMNFAMILY IF NOT EXISTS users (key varchar PRIMARY KEY,full_name varchar, birth_date int,state varchar)";
        q = entityManager.createNativeQuery(colFamilySql);
        q.executeUpdate();
        Assert.assertTrue(CassandraCli.columnFamilyExist("users", "KunderaExamples"));

        // Add indexes
        String idxSql = "CREATE INDEX IF NOT EXISTS ON users (birth_date)";
        q = entityManager.createNativeQuery(idxSql);
        q.executeUpdate();
        idxSql = "CREATE INDEX IF NOT EXISTS ON users (state)";
        q = entityManager.createNativeQuery(idxSql);
        q.executeUpdate();
        // insert users.
        String insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('bsanderson', 'Brandon Sanderson', 1975, 'UT')";
        q = entityManager.createNativeQuery(insertSql);
        q.executeUpdate();

        // select key and state
        String selectSql = "SELECT key, state FROM users";
        q = entityManager.createNativeQuery(selectSql);

        List<Object[]> results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        Assert.assertEquals("bsanderson", results.get(0)[0]);
        Assert.assertEquals("UT", results.get(0)[1]);
      //  Assert.assertEquals(null, ((Map) results.get(0)).get("full_name"));

        // insert users.
        insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('prothfuss', 'Patrick Rothfuss', 1973, 'WI')";
        q = entityManager.createNativeQuery(insertSql);
        q.getResultList();

        insertSql = "INSERT INTO users (key, full_name, birth_date, state) VALUES ('htayler', 'Howard Tayler', 1968, 'UT')";
        q = entityManager.createNativeQuery(insertSql);
        q.getResultList();

        // select all
        String selectAll = "SELECT * FROM users WHERE state='UT' AND birth_date > 1970 ALLOW FILTERING";
        q = entityManager.createNativeQuery(selectAll);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        Assert.assertEquals("bsanderson", results.get(0)[0]);
        Assert.assertEquals("UT", results.get(0)[1]);
        Assert.assertEquals("Brandon Sanderson", results.get(0)[2]);
        Assert.assertEquals(new Integer(1975), results.get(0)[3]);

        String updateSql = "UPDATE users SET full_name = 'update' WHERE key = 'bsanderson'";
        q = entityManager.createNativeQuery(updateSql);
        q.getResultList();

        selectAll = "SELECT * FROM users WHERE state='UT' AND birth_date > 1970 ALLOW FILTERING";
        q = entityManager.createNativeQuery(selectAll);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        Assert.assertEquals("bsanderson", results.get(0)[0]);
        Assert.assertEquals("UT", results.get(0)[1]);
        Assert.assertEquals("update", results.get(0)[2]);
        Assert.assertEquals(new Integer(1975), results.get(0)[3]);

        entityManager.close();
        emf.close();
    }

    @Test
    public void testMetadataQueries()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("dsscalar");
        EntityManager entityManager = emf.createEntityManager();
        String useNativeSql = "SELECT keyspace_name,table_name,column_name,kind,type FROM system_schema.columns"
                + " WHERE keyspace_name = 'KunderaExamples' AND table_name = 'PERSON'";
        Query q = entityManager.createNativeQuery(useNativeSql);
        List<Object[]> results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(6, results.size());

        Assert.assertEquals("KunderaExamples", results.get(0)[0]);
        Assert.assertEquals("PERSON", results.get(0)[1]);
        Assert.assertEquals("AGE", results.get(0)[2]);
        Assert.assertEquals("regular", results.get(0)[3]);
        Assert.assertEquals("int", results.get(0)[4]);

        useNativeSql = "SELECT keyspace_name, durable_writes FROM system_schema.keyspaces WHERE keyspace_name = 'KunderaExamples'";
        q = entityManager.createNativeQuery(useNativeSql);
        results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        Assert.assertEquals("KunderaExamples", results.get(0)[0]);
        Assert.assertEquals(true, results.get(0)[1]);

        useNativeSql = "SELECT COUNT(*) FROM system_schema.columns WHERE keyspace_name = 'KunderaExamples'AND table_name = 'PERSON'";
        q = entityManager.createNativeQuery(useNativeSql);

        final List<Object> resultsSingle = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, resultsSingle.size());
        Assert.assertEquals(6l, resultsSingle.get(0));

        entityManager.close();
        emf.close();
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    /**
     * Prepare data.
     * 
     * @param rowKey
     *            the row key
     * @param age
     *            the age
     * @return the person cassandra
     */
    private static PersonCassandra prepareData(String rowKey, int age)
    {
        PersonCassandra o = new PersonCassandra();
        o.setPersonId(rowKey);
        o.setPersonName("karthik");
        o.setAge(age);
        o.setDay(Day.friday);
        o.setMonth(Month.MAY);
        return o;
    }

}
