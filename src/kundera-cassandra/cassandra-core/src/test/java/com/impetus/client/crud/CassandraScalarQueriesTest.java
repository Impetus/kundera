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

    /** The entity manager factory. */
    private static EntityManagerFactory entityManagerFactory;

    /** The entity manager. */
    private static EntityManager entityManager;

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
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("genericCassandraTest", propertyMap);
        EntityManager em = emf.createEntityManager();

        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        emf.close();
        em.close();

        entityManagerFactory = Persistence.createEntityManagerFactory("CassandraScalarQueriesTest");
        entityManager = entityManagerFactory.createEntityManager();
    }

    /**
     * Test scalar query.
     */
    @Test
    public void testSelectQueries()
    {
        String qry = "Select key, \"PERSON_NAME\" from \"PERSONCASSANDRA\" where key = '1'";

        Query q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("1", ((Map) persons.get(0)).get("key"));
        Assert.assertEquals("karthik", ((Map) persons.get(0)).get("PERSON_NAME"));

        qry = "Select * from \"PERSONCASSANDRA\" where key = '1'";
        q = entityManager.createNativeQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("1", ((Map) persons.get(0)).get("key"));
        Assert.assertEquals("karthik", ((Map) persons.get(0)).get("PERSON_NAME"));
        Assert.assertEquals("MAY", ((Map) persons.get(0)).get("MONTH_ENUM"));
        Assert.assertEquals(10, ((Map) persons.get(0)).get("AGE"));

        qry = "Select * from \"PERSONCASSANDRA\"";
        q = entityManager.createNativeQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(3, persons.size());
    }

    /**
     * Test on column family.
     */
    @Test
    public void testCreateAndUpdateQueries()
    {
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
        List results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("bsanderson", ((Map) results.get(0)).get("key"));
        Assert.assertEquals("UT", ((Map) results.get(0)).get("state"));
        Assert.assertEquals(null, ((Map) results.get(0)).get("full_name"));

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
        Assert.assertEquals("bsanderson", ((Map) results.get(0)).get("key"));
        Assert.assertEquals("UT", ((Map) results.get(0)).get("state"));
        Assert.assertEquals("Brandon Sanderson", ((Map) results.get(0)).get("full_name"));
        Assert.assertEquals(new Integer(1975), ((Map) results.get(0)).get("birth_date"));

        String updateSql = "UPDATE users SET full_name = 'update' WHERE key = 'bsanderson'";
        q = entityManager.createNativeQuery(updateSql);
        q.getResultList();

        selectAll = "SELECT * FROM users WHERE state='UT' AND birth_date > 1970 ALLOW FILTERING";
        q = entityManager.createNativeQuery(selectAll);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("bsanderson", ((Map) results.get(0)).get("key"));
        Assert.assertEquals("UT", ((Map) results.get(0)).get("state"));
        Assert.assertEquals("update", ((Map) results.get(0)).get("full_name"));
        Assert.assertEquals(new Integer(1975), ((Map) results.get(0)).get("birth_date"));

    }

    @Test
    public void testMetadataQueries()
    {
        String useNativeSql = "SELECT keyspace_name,table_name,column_name,kind,type FROM system_schema.columns"
                + " WHERE keyspace_name = 'KunderaExamples' AND table_name = 'PERSONCASSANDRA'";
        Query q = entityManager.createNativeQuery(useNativeSql);
        List results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(8, results.size());
        Assert.assertEquals("KunderaExamples", ((Map) results.get(0)).get("keyspace_name"));
        Assert.assertEquals("PERSONCASSANDRA", ((Map) results.get(0)).get("table_name"));
        Assert.assertEquals("AGE", ((Map) results.get(0)).get("column_name"));
        Assert.assertEquals("static", ((Map) results.get(0)).get("kind"));
        Assert.assertEquals("int", ((Map) results.get(0)).get("type"));

        useNativeSql = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'KunderaExamples'";
        q = entityManager.createNativeQuery(useNativeSql);
        results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("KunderaExamples", ((Map) results.get(0)).get("keyspace_name"));
        Assert.assertEquals(true, ((Map) results.get(0)).get("durable_writes"));

        useNativeSql = "SELECT COUNT(*) FROM system_schema.columns WHERE keyspace_name = 'KunderaExamples'AND table_name = 'PERSONCASSANDRA'";
        q = entityManager.createNativeQuery(useNativeSql);
        results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(8l, ((Map) results.get(0)).get("count"));

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
