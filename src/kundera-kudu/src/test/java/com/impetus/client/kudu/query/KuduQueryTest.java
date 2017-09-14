/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu.query;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.kudu.entities.Person;

import junit.framework.Assert;

/**
 * The Class KuduQueryTest.
 */
public class KuduQueryTest
{
    /** The Constant KUDU_PU. */
    private static final String KUDU_PU = "kudu";

    /** The Constant T. */
    private static final boolean T = true;

    /** The Constant F. */
    private static final boolean F = false;

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(KUDU_PU);
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
        createPersons();
    }

    /**
     * Test select.
     */
    @Test
    public void testSelect()
    {

        Query query = em.createQuery("Select p from Person p");
        List<Person> results = query.getResultList();
        Assert.assertEquals(5, results.size());
        assertResults(results, T, T, T, T, T);

        query = em.createQuery("Select p.personName from Person p where p.age = 20");
        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Person person = results.get(0);
        Assert.assertNotNull(person);
        Assert.assertEquals("karthik", person.getPersonName());
        Assert.assertNull(person.getPersonId());
        Assert.assertNull(person.getSalary());
        Assert.assertEquals(0, person.getAge());

        query = em.createQuery("Select p from Person p where p.age >= 20");
        results = query.getResultList();
        Assert.assertEquals(4, results.size());
        assertResults(results, F, T, T, T, T);

        query = em.createQuery("Select p from Person p where p.age > 20");
        results = query.getResultList();
        Assert.assertEquals(3, results.size());
        assertResults(results, F, F, T, T, T);

        query = em.createQuery("Select p from Person p where p.personId = '101'");
        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        assertResults(results, T, F, F, F, F);

        query = em.createQuery("Select p.age, p.salary from Person p where p.age >= 20 and p.age <= 40");
        results = query.getResultList();
        Assert.assertEquals(3, results.size());

        query = em.createQuery("Select p.age, p.salary from Person p where p.age >= ?1 and p.age <= ?2");
        query.setParameter(1, 20);
        query.setParameter(2, 40);
        results = query.getResultList();
        Assert.assertEquals(3, results.size());

        query = em.createQuery("Select p.age, p.salary from Person p where p.age >= :age1 and p.age <= :age2");
        query.setParameter("age1", 20);
        query.setParameter("age2", 40);
        results = query.getResultList();
        Assert.assertEquals(3, results.size());

        query = em.createQuery("Select p from Person p where p.personId in (101, 104)");
        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        assertResults(results, T, F, F, T, F);
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
        deletePersons();
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
        if (emf != null)
        {
            emf.close();
        }
    }

    /**
     * Creates the persons.
     */
    private static void createPersons()
    {
        Person person1 = new Person("101", "dev", 10, 10000.1);
        em.persist(person1);
        Person person2 = new Person("102", "karthik", 20, 20000.2);
        em.persist(person2);
        Person person3 = new Person("103", "pg", 30, 30000.3);
        em.persist(person3);
        Person person4 = new Person("104", "amit", 40, 40000.4);
        em.persist(person4);
        Person person5 = new Person("105", "vivek", 50, 50000.5);
        em.persist(person5);
    }

    /**
     * Delete persons.
     */
    private static void deletePersons()
    {
        Query query = em.createQuery("Select p from Person p");
        List<Person> results = query.getResultList();
        for (Person p : results)
        {
            em.remove(p);
        }
    }

    /**
     * Validate person1.
     *
     * @param p
     *            the p
     */
    protected void validatePerson1(Person p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("101", p.getPersonId());
        Assert.assertEquals("dev", p.getPersonName());
        Assert.assertEquals(10, p.getAge());
        Assert.assertEquals(10000.1, p.getSalary());
    }

    /**
     * Validate person2.
     *
     * @param p
     *            the p
     */
    protected void validatePerson2(Person p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("102", p.getPersonId());
        Assert.assertEquals("karthik", p.getPersonName());
        Assert.assertEquals(20, p.getAge());
        Assert.assertEquals(20000.2, p.getSalary());
    }

    /**
     * Validate person3.
     *
     * @param p
     *            the p
     */
    protected void validatePerson3(Person p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("103", p.getPersonId());
        Assert.assertEquals("pg", p.getPersonName());
        Assert.assertEquals(30, p.getAge());
        Assert.assertEquals(30000.3, p.getSalary());
    }

    /**
     * Validate person4.
     *
     * @param p
     *            the p
     */
    protected void validatePerson4(Person p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("104", p.getPersonId());
        Assert.assertEquals("amit", p.getPersonName());
        Assert.assertEquals(40, p.getAge());
        Assert.assertEquals(40000.4, p.getSalary());
    }

    /**
     * Validate person5.
     *
     * @param p
     *            the p
     */
    protected void validatePerson5(Person p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("105", p.getPersonId());
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertEquals(50, p.getAge());
        Assert.assertEquals(50000.5, p.getSalary());
    }

    /**
     * Assert results.
     *
     * @param results
     *            the results
     * @param foundPerson1
     *            the found person1
     * @param foundPerson2
     *            the found person2
     * @param foundPerson3
     *            the found person3
     * @param foundPerson4
     *            the found person4
     * @param foundPerson5
     *            the found person5
     */
    protected void assertResults(List<Person> results, boolean foundPerson1, boolean foundPerson2, boolean foundPerson3,
            boolean foundPerson4, boolean foundPerson5)
    {
        for (Person person : results)
        {
            switch (person.getPersonId())
            {
            case "101":
                if (foundPerson1)
                    validatePerson1(person);
                else
                    Assert.fail();
                break;
            case "102":
                if (foundPerson2)
                    validatePerson2(person);
                else
                    Assert.fail();
                break;
            case "103":
                if (foundPerson3)
                    validatePerson3(person);
                else
                    Assert.fail();
                break;
            case "104":
                if (foundPerson4)
                    validatePerson4(person);
                else
                    Assert.fail();
                break;
            case "105":
                if (foundPerson5)
                    validatePerson5(person);
                else
                    Assert.fail();
                break;
            }
        }
    }

}
