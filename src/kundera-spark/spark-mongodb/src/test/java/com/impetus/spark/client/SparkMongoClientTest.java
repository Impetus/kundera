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
package com.impetus.spark.client;

import java.net.UnknownHostException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.spark.entities.Person;
import com.impetus.client.spark.tests.SparkBaseTest;
import com.impetus.client.spark.utils.SparkTestingUtils;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/**
 * The Class MongoClientTest.
 * 
 * @author devender.yadav
 */
public class SparkMongoClientTest extends SparkBaseTest
{

    /** The Constant MONGO_PU. */
    private static final String MONGO_PU = "spark_mongo_pu";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The entity manager. */
    private EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(MONGO_PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * Spark mongo test.
     */
    @Test
    public void sparkMongoTest()
    {
        testPersist();
        testQuery();
        testSaveIntermediateResult();
    }

    /**
     * Test persist.
     */
    public void testPersist()
    {

        Person person1 = getPerson("1", "dev", 22, 30000.5);
        Person person2 = getPerson("2", "pg", 23, 40000.6);
        Person person3 = getPerson("3", "kpm", 24, 50000.7);

        em.clear();
        em.persist(person1);
        em.persist(person2);
        em.persist(person3);

        Person p = em.find(Person.class, "1");
        validatePerson1(p);

        p = em.find(Person.class, "2");
        validatePerson2(p);

        p = em.find(Person.class, "3");
        validatePerson3(p);
    }

    /**
     * Test query.
     */
    public void testQuery()
    {
        List<Person> results = em.createNativeQuery("select * from spark_person").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, true, true, true);

        results = em.createNativeQuery("select * from spark_person where salary > 35000").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, false, true, true);

        results = em.createNativeQuery("select * from spark_person where salary > 35000 and age = 23").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertResults(results, false, true, false);

        results = em.createNativeQuery("select * from spark_person where personName like 'kp%'").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertResults(results, false, false, true);

        List aggregateResults = em.createNativeQuery("select sum(salary) from spark_person").getResultList();
        Assert.assertNotNull(aggregateResults);

        aggregateResults = em.createNativeQuery("select count(*) from spark_person where salary > 30000")
                .getResultList();
        Assert.assertNotNull(aggregateResults.size());

    }

    /**
     * Test save intermediate result.
     */
    public void testSaveIntermediateResult()
    {
        String sqlString = "INSERT INTO fs.[src/test/resources/testspark_csv] AS CSV FROM (select * from spark_person)";
        Query q = em.createNativeQuery(sqlString, Person.class);
        q.executeUpdate();

        sqlString = "INSERT INTO fs.[src/test/resources/testspark_json] AS JSON FROM (select * from spark_person)";
        q = em.createNativeQuery(sqlString, Person.class);
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
        em.close();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        SparkTestingUtils.recursivelyCleanDir("src/test/resources/testspark_csv");
        SparkTestingUtils.recursivelyCleanDir("src/test/resources/testspark_json");
        dropDB();
        emf.close();
        emf = null;
    }

    private static void dropDB() throws UnknownHostException
    {
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
        DB db = mongoClient.getDB("sparktest");
        db.dropDatabase();
    }

}
