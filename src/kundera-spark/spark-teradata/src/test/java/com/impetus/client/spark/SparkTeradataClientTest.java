/******************************************************************************* 	
 * * Copyright 2015 Impetus Infotech. 	
 * * 	
 * * Licensed under the Apache License, Version 2.0 (the "License"); 	
 * * you may not use this file except in compliance with the License. 	
 * * You may obtain a copy of the License at 	
 * * 	
 * * http://www.apache.org/licenses/LICENSE2.0 	
 * * 	
 * * Unless required by applicable law or agreed to in writing, software 	
 * * distributed under the License is distributed on an "AS IS" BASIS, 	
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 	
 * * See the License for the specific language governing permissions and 	
 * * limitations under the License. 	
 ******************************************************************************/
package com.impetus.client.spark;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.spark.tests.SparkBaseTest;

/**
 * The Class SparkTeradataClientTest.
 * 
 * @author amitkumar
 */
public class SparkTeradataClientTest extends SparkBaseTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The entity manager. */
    private EntityManager em;

    /** The pu. */
    private static final String PU = "teradata_spark_pu";

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
  //  @BeforeClass
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
   // @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * Test teradata with spark.
     */
   // @Test
    public void testTeradataWithSpark()
    {
        testQuery();
    }

    /**
     * Test query.
     */
    private void testQuery()
    {
        List<Person> results = em.createNativeQuery("select * from person").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(4, results.size());
        matchResults(results, 4);

        results = em.createNativeQuery("select * from person where salary > 80").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        matchResults(results, 2);

        results = em.createNativeQuery("select * from person where firstName like 'Am%'").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        matchResults(results, 1);

        List aggregateResults = em.createNativeQuery("select sum(salary) from person").getResultList();
        Assert.assertNotNull(aggregateResults);

        aggregateResults = em.createNativeQuery("select count(*) from person where salary > 10.0").getResultList();
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
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
   // @After
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
    //@AfterClass
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
    private void matchResults(List<Person> results, int count)
    {
        Assert.assertEquals(count, results.size());
        for (Person person : results)
        {
            matchPerson(person, person.getId());
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
    private void matchPerson(Person person, int personId)
    {
        switch (personId)
        {
        case 1:
            Assert.assertEquals(1, person.getId());
            Assert.assertEquals("Amit", person.getFirstName());
            Assert.assertEquals(100, person.getSalary());
            break;

        case 2:
            Assert.assertEquals(2, person.getId());
            Assert.assertEquals("John", person.getFirstName());
            Assert.assertEquals(200, person.getSalary());
            break;
        case 3:
            Assert.assertEquals(3, person.getId());
            Assert.assertEquals("Marty", person.getFirstName());
            Assert.assertEquals(50, person.getSalary());
            break;
        case 4:
            Assert.assertEquals(4, person.getId());
            Assert.assertEquals("Marty Cole", person.getFirstName());
            Assert.assertEquals(10, person.getSalary());
            break;
        default:
            Assert.assertTrue(false);
        }
    }
}