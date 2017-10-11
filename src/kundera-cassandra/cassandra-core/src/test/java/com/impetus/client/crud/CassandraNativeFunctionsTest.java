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

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.crud.PersonCassandra.Day;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * The Class CassandraNativeFunctionsTest.
 * 
 * @author: karthikp.manchala
 */
public class CassandraNativeFunctionsTest
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
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("genericCassandraTest", propertyMap);
        EntityManager em = emf.createEntityManager();

        Object p1 = prepareData("1", 10, "karthik");
        Object p2 = prepareData("2", 20, "dev");
        Object p3 = prepareData("3", 15, "karthik");
        Object p4 = prepareData("4", 25, "dev");
        Object p5 = prepareData("5", 30, "karthik");

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        em.persist(p4);
        em.persist(p5);

        emf.close();
        em.close();

        entityManagerFactory = Persistence.createEntityManagerFactory("CassandraScalarQueriesTest");
        entityManager = entityManagerFactory.createEntityManager();
    }

    /**
     * Test native aggregations.
     */
    @Test
    public void testNativeAggregations()
    {
        String qry = "Select min(\"AGE\") as min_age, count(*) from \"PERSONCASSANDRA\"";

        Query q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(10, ((Map) persons.get(0)).get("min_age"));
        Assert.assertEquals(5l, ((Map) persons.get(0)).get("count"));

        qry = "Select max(\"AGE\") from \"PERSONCASSANDRA\"";
        q = entityManager.createNativeQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(30, ((Map) persons.get(0)).get("system.max(AGE)"));

        qry = "Select sum(\"AGE\") from \"PERSONCASSANDRA\"";
        q = entityManager.createNativeQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(100, ((Map) persons.get(0)).get("system.sum(AGE)"));

        qry = "Select avg(\"AGE\") from \"PERSONCASSANDRA\"";
        q = entityManager.createNativeQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(20, ((Map) persons.get(0)).get("system.avg(AGE)"));
    }

    /**
     * Test user defined functions.
     */
    @Test
    public void testUserDefinedFunctions()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        Query q = entityManager.createNativeQuery(useNativeSql);
        q.executeUpdate();
        // create user defined function
        String udf = "CREATE OR REPLACE FUNCTION fLog (input int) " + "CALLED ON NULL INPUT " + "RETURNS double "
                + "LANGUAGE java AS 'return Double.valueOf(Math.log(input.intValue()));'";
        q = entityManager.createNativeQuery(udf);
        q.executeUpdate();
        // Assert values
        String qry = "Select fLog(\"AGE\") from \"PERSONCASSANDRA\" where \"personId\" = '1'";
        q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        // log 10 base e
        Assert.assertEquals(2.302585092994046, ((Map) persons.get(0)).get("KunderaExamples.flog(AGE)"));
    }

    /**
     * Test user defined aggregation functions.
     */
    @Test
    public void testUserDefinedAggregationFunctions()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        Query q = entityManager.createNativeQuery(useNativeSql);
        q.executeUpdate();
        // create user defined function to return state for aggregation function
        String udaf = "CREATE FUNCTION state_group_and_total( state map<text, int>, name text, age int )"
                + "CALLED ON NULL INPUT " + "RETURNS map<text, int> "
                + "LANGUAGE java AS 'Integer count = (Integer) state.get(name); " + "if (count == null) count = age; "
                + "else count = count + age; " + "state.put(name, count); return state; ' ;";
        q = entityManager.createNativeQuery(udaf);
        q.executeUpdate();

        // create aggregation function
        udaf = "CREATE OR REPLACE AGGREGATE group_and_total(text, int) " + "SFUNC state_group_and_total "
                + "STYPE map<text, int>" + "INITCOND {};";
        q = entityManager.createNativeQuery(udaf);
        q.executeUpdate();
        // Assert values
        String qry = "Select group_and_total(\"PERSON_NAME\", \"AGE\") from \"PERSONCASSANDRA\"";
        q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        // log 10 base e
        Assert.assertEquals(45,
                ((Map) ((Map) persons.get(0)).get("KunderaExamples.group_and_total(PERSON_NAME, AGE)")).get("dev"));
        Assert.assertEquals(55,
                ((Map) ((Map) persons.get(0)).get("KunderaExamples.group_and_total(PERSON_NAME, AGE)")).get("karthik"));
    }

    /**
     * Test select and insert json.
     */
    @Test
    public void testSelectAndInsertJSON()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        Query q = entityManager.createNativeQuery(useNativeSql);
        q.executeUpdate();
        // create user defined function
        String insertJSON = "Insert into \"PERSONCASSANDRA\" JSON '{\"\\\"PERSON_NAME\\\"\": \"kundera\", \"\\\"AGE\\\"\": 123, \"\\\"personId\\\"\":\"abc\" }'";
        q = entityManager.createNativeQuery(insertJSON);
        q.executeUpdate();
        // Assert values
        String qry = "Select JSON * from \"PERSONCASSANDRA\"";
        q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(6, persons.size());
        q = entityManager.createNativeQuery("Delete from \"PERSONCASSANDRA\" where \"personId\" = 'abc'");
        q.executeUpdate();
    }

    /**
     * Test to json.
     */
    @Test
    public void testToJSON()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        Query q = entityManager.createNativeQuery(useNativeSql);
        q.executeUpdate();
        String qry = "Select \"personId\", toJson(\"PERSON_NAME\") as person_name from \"PERSONCASSANDRA\"";
        q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(5, persons.size());
    }

    /**
     * Test from json.
     */
    @Test
    public void testFromJSON()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        Query q = entityManager.createNativeQuery(useNativeSql);
        q.executeUpdate();
        String qry = "UPDATE \"PERSONCASSANDRA\" SET \"AGE\" = fromJson('500') WHERE \"personId\" = fromJson('\"3\"')";
        q = entityManager.createNativeQuery(qry);
        q.executeUpdate();

        // Assertions
        qry = "Select * from \"PERSONCASSANDRA\" where \"personId\" = '3'";
        q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("3", ((Map) persons.get(0)).get("personId"));
        Assert.assertEquals("karthik", ((Map) persons.get(0)).get("PERSON_NAME"));
        Assert.assertEquals("MAY", ((Map) persons.get(0)).get("MONTH_ENUM"));
        Assert.assertEquals(500, ((Map) persons.get(0)).get("AGE"));
    }

    /**
     * Test in clause on primary key.
     */
    @Test
    public void testINClauseOnPrimaryKey()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        Query q = entityManager.createNativeQuery(useNativeSql);
        q.executeUpdate();

        String qry = "Select * from \"PERSONCASSANDRA\" where \"personId\" IN ('1', '3', '5', '7')";
        q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(3, persons.size());
    }

    /**
     * Test in clause on composite partition and clustering keys.
     */
    @Test
    public void testINClauseOnCompositePartitionAndClusteringKeys()
    {
        // prepare data for querying
        prepareDataWithCompositePartitionAndClusteringKeys();

        // query and assert
        String qry = "select * from kundera where k_part_one IN ('kar','dev', 'amit') and k_part_two IN (1, 5, 7, 9, 13, 50)";
        Query q = entityManager.createNativeQuery(qry);
        List persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(2, persons.size());

        qry = "select * from kundera where k_part_one IN ('kar','dev', 'amit') and k_part_two IN (1, 5, 7, 9, 13, 50) "
                + "and k_clust_one IN ('cluster11', 'cluster13', 'cluster14')";
        q = entityManager.createNativeQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());

        qry = "select * from kundera where k_part_one IN ('kar','dev', 'amit')";
        q = entityManager.createNativeQuery(qry);
        try
        {
            persons = q.getResultList();
            Assert.fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals("javax.persistence.PersistenceException: com.impetus.kundera.KunderaException: "
                    + "InvalidRequestException(why:Cannot execute this query as it might "
                    + "involve data filtering and thus may have unpredictable performance. "
                    + "If you want to execute this query despite the performance "
                    + "unpredictability, use ALLOW FILTERING)", e.getMessage());
        }
    }

    /**
     * Prepare data with composite partition and clustering keys.
     */
    private void prepareDataWithCompositePartitionAndClusteringKeys()
    {
        String useNativeSql = "USE " + "\"KunderaExamples\"";
        Query q = entityManager.createNativeQuery(useNativeSql);
        q.executeUpdate();
        String qry = "create table kundera (k_part_one text, k_part_two int, "
                + "k_clust_one text, k_clust_two int, k_clust_three text, data text, "
                + "PRIMARY KEY((k_part_one,k_part_two), k_clust_one, k_clust_two, k_clust_three))";
        q = entityManager.createNativeQuery(qry);
        q.executeUpdate();

        qry = "insert into kundera (k_part_one, k_part_two, k_clust_one, k_clust_two, k_clust_three, data)"
                + "VALUES ('kar', 13, 'cluster11', 21, 'cluster31', 'some random data')";
        q = entityManager.createNativeQuery(qry);
        q.executeUpdate();

        qry = "insert into kundera (k_part_one, k_part_two, k_clust_one, k_clust_two, k_clust_three, data)"
                + "VALUES ('dev', 7, 'cluster12', 22, 'cluster32', 'some more random data')";
        q = entityManager.createNativeQuery(qry);
        q.executeUpdate();

        qry = "insert into kundera (k_part_one, k_part_two, k_clust_one, k_clust_two, k_clust_three, data)"
                + "VALUES ('pg', 9, 'cluster13', 23, 'cluster33', 'random data')";
        q = entityManager.createNativeQuery(qry);
        q.executeUpdate();
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
        entityManagerFactory.close();
        entityManager.close();
    }

    /**
     * Prepare data.
     * 
     * @param rowKey
     *            the row key
     * @param age
     *            the age
     * @param name
     *            the name
     * @return the person cassandra
     */
    private static PersonCassandra prepareData(String rowKey, int age, String name)
    {
        PersonCassandra o = new PersonCassandra();
        o.setPersonId(rowKey);
        o.setPersonName(name);
        o.setAge(age);
        o.setDay(Day.friday);
        o.setMonth(Month.MAY);
        return o;
    }

}
