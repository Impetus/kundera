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
package com.impetus.client.spark;

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

import com.impetus.client.spark.tests.SparkBaseTest;

/**
 * The Class SparkHiveQueryTest.
 * 
 * @author amitkumar
 */
public class SparkHiveClientTest extends SparkBaseTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The entity manager. */
    private EntityManager em;

    /** The pu. */
    private static final String PU = "spark_hive_PU";

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
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
     * Test hive with spark.
     */
    @Test
    public void testHiveWithSpark()
    {
        testPersist();
        persistData();
        testQuery();
        testSaveIntermediateResult();
    }

    /**
     * Test query.
     */
    private void testQuery()
    {
        List<PersonHive> results = em.createNativeQuery("select * from sparktest.person").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        matchResults(results, 4);

        results = em.createNativeQuery("select * from sparktest.person where salary > 80").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        matchResults(results, 2);

        // results =
        // em.createNativeQuery("select * from sparktest.person where salary > 50 and age = 60").getResultList();
        // Assert.assertNotNull(results);
        // Assert.assertEquals(1, results.size());
        // matchPerson(results.get(0), 2);

        results = em.createNativeQuery("select * from sparktest.person where personName like 'Am%'").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        matchResults(results, 1);

        List aggregateResults = em.createNativeQuery("select sum(salary) from sparktest.person").getResultList();
        Assert.assertNotNull(aggregateResults);

        aggregateResults = em.createNativeQuery("select count(*) from sparktest.person where salary > 10.0")
                .getResultList();
        Assert.assertNotNull(aggregateResults.size());

        results = em.createNativeQuery("select * from person").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        matchResults(results, 4);

        results = em.createNativeQuery("select * from person where salary > 80").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        matchResults(results, 2);

        results = em.createNativeQuery("select * from person where salary between 20.0 and 90").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        matchPerson(results.get(0), 3);

    }

    /**
     * Persist data.
     */
    private void persistData()
    {
        em.persist(createPerson("2", "John", 200.00));
        em.persist(createPerson("3", "Marty", 50.00));
        em.persist(createPerson("4", "Marty Cole", 10.00));
    }

    /**
     * Test persist.
     */
    private void testPersist()
    {
        PersonHive person = createPerson("1", "Amit", 100.00);
        em.persist(person);

        List<PersonHive> results = em.createNativeQuery("select * from sparktest.person").getResultList();
        PersonHive personHive = results.get(0);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals("1", personHive.getPersonId());
        Assert.assertEquals("Amit", personHive.getPersonName());
        Assert.assertEquals(100.00, personHive.getSalary());
    }

    /**
     * Creates the person.
     * 
     * @param id
     *            the id
     * @param name
     *            the name
     * @param salary
     *            the salary
     * @return the person hive
     */
    private PersonHive createPerson(String id, String name, Double salary)
    {
        PersonHive person = new PersonHive();

        person.setPersonId(id);
        person.setPersonName(name);
        person.setSalary(salary);

        return person;
    }

    /**
     * Test save intermediate result.
     */
    private void testSaveIntermediateResult()
    {
        String sqlString = "INSERT INTO hive.sparktest.IntermediatePerson FROM (select * from person)";
        Query q = em.createNativeQuery(sqlString, PersonHive.class);
        q.executeUpdate();

        List<PersonHive> results = em.createNativeQuery("select * from sparktest.IntermediatePerson").getResultList();
        matchResults(results, 4);

        sqlString = "INSERT INTO fs.[src/test/resources/testspark_csv] AS CSV FROM (select * from person)";
        q = em.createNativeQuery(sqlString, PersonHive.class);
        q.executeUpdate();

        sqlString = "INSERT INTO fs.[src/test/resources/testspark_json] AS JSON FROM (select * from person)";
        q = em.createNativeQuery(sqlString, PersonHive.class);
        q.executeUpdate();
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
        em.createNativeQuery("TRUNCATE TABLE person").getResultList();
        em.createNativeQuery("TRUNCATE TABLE intermediatePerson").getResultList();
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
        emf.close();
        emf = null;
    }

    /**
     * Match results.
     * 
     * @param results
     *            the results
     * @param count
     *            the count
     */
    private void matchResults(List<PersonHive> results, int count)
    {
        int flag = 1;
        Assert.assertEquals(count, results.size());
        for (PersonHive person : results)
        {
            matchPerson(person, flag++);
        }
    }

    /**
     * Match person.
     * 
     * @param person
     *            the person
     * @param personId
     *            the person id
     */
    private void matchPerson(PersonHive person, int personId)
    {
        switch (personId)
        {
        case 1:
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("Amit", person.getPersonName());
            Assert.assertEquals(100.0, person.getSalary());
            break;

        case 2:
            Assert.assertEquals("2", person.getPersonId());
            Assert.assertEquals("John", person.getPersonName());
            Assert.assertEquals(200.0, person.getSalary());
            break;
        case 3:
            Assert.assertEquals("3", person.getPersonId());
            Assert.assertEquals("Marty", person.getPersonName());
            Assert.assertEquals(50.0, person.getSalary());
            break;
        case 4:
            Assert.assertEquals("4", person.getPersonId());
            Assert.assertEquals("Marty Cole", person.getPersonName());
            Assert.assertEquals(10.0, person.getSalary());
            break;
        default:
            Assert.assertTrue(false);
        }
    }
}
