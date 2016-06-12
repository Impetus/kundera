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
package com.impetus.client.es;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.es.PersonES.Day;

/**
 * The Class ESNestedQueryTest.
 * 
 * @author Amit Kumar
 */
public class ESNestedQueryTest
{
    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The node. */
    private static Node node = null;

    /** The person1. */
    private PersonES person1, person2, person3, person4;

    /**
     * Before class setup.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9300);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    /**
     * Setup.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Before
    public void setup() throws InterruptedException
    {

        emf = Persistence.createEntityManagerFactory("es-pu");
        em = emf.createEntityManager();
        init();
    }

    /**
     * Inits the test data.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    private void init() throws InterruptedException
    {

        person1 = createPerson("1", 20, "Amit", 100.0);
        person2 = createPerson("2", 10, "Dev", 200.0);
        person3 = createPerson("3", 30, "Karthik", 300.0);
        person4 = createPerson("4", 40, "Pragalbh", 400.0);
        waitThread();
    }

    /**
     * Creates the person and persist them.
     * 
     * @param id
     *            the id
     * @param age
     *            the age
     * @param name
     *            the name
     * @param salary
     *            the salary
     * @return the person es
     */
    private PersonES createPerson(String id, int age, String name, Double salary)
    {
        PersonES person = new PersonES();
        person.setAge(age);
        person.setDay(Day.FRIDAY);
        person.setPersonId(id);
        person.setPersonName(name);
        person.setSalary(salary);
        em.persist(person);

        return person;
    }

    /**
     * Test nested query.
     */
    @Test
    public void testNestedQuery()
    {
        testQueryWithAndClause();
        testQueryWithMultiAndClause();
        testQueryWithOrClause();
        testQueryWithMultiOrClause();
        testQueryWithNestedAndOrClause();
        testMinAggregationWithNestedAndOrClause();
        testMaxAggregationWithNestedAndOrClause();
        testMinAggregationWithNestedAndOrClause();
        testMinAggregationWithNestedAndOr();
    }

    /**
     * Test query with and clause.
     */
    public void testQueryWithAndClause()
    {
        String nestedQquery = "Select p from PersonES p where p.personName = 'karthik' AND p.personName = 'pragalbh'";
        Query query = em.createQuery(nestedQquery);
        List<PersonES> resultList = query.getResultList();

        Assert.assertEquals(0, resultList.size());
    }

    /**
     * Test query with multi and clause.
     */
    public void testQueryWithMultiAndClause()
    {
        String nestedQquery = "Select p from PersonES p where p.age > 0 AND p.age < 35 AND p.salary > 150";
        Query query = em.createQuery(nestedQquery);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person2, person3);
    }

    /**
     * Test query with or clause.
     */
    public void testQueryWithOrClause()
    {
        String nestedQquery = "Select p from PersonES p where p.personName = 'karthik' OR p.personName = 'pragalbh'";
        Query query = em.createQuery(nestedQquery);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person3, person4);
    }

    /**
     * Test query with multi or clause.
     */
    public void testQueryWithMultiOrClause()
    {
        String nestedQquery = "Select p from PersonES p where p.personName = 'amit' OR p.age < 15 OR p.age > 35";
        Query query = em.createQuery(nestedQquery);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person1, person2, person4);
    }

    /**
     * Test query with nested and or clause.
     */
    public void testQueryWithNestedAndOrClause()
    {
        String nestedQuery = "Select p from PersonES p where p.age > 0 AND (p.salary > 350 and (p.personName = :name OR p.personName = 'pragalbh'))";

        Query query = em.createQuery(nestedQuery);
        query.setParameter("name", "karthik");
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person4);
    }

    /**
     * Test max aggregation with nested and or clause.
     */
    public void testMaxAggregationWithNestedAndOrClause()
    {
        String nestedQuery = "Select max(p.age) from PersonES p where p.age > 0 AND (p.salary > 250 and (p.personName = 'karthik' OR p.personName = 'pragalbh'))";

        Query query = em.createQuery(nestedQuery);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
    }

    /**
     * Test min aggregation with nested and or clause.
     */
    public void testMinAggregationWithNestedAndOrClause()
    {
        String invalidQueryWithAndClause = "Select min(p.age) from PersonES p where p.age > 0 AND (p.personName = 'amit' OR p.personName = 'dev')";
        Query nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
        List resultList = nameQuery.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(10.0, resultList.get(0));
    }

    /**
     * Test min aggregation with nested and or.
     */
    public void testMinAggregationWithNestedAndOr()
    {
        String invalidQueryWithAndClause = "Select min(p.age) from PersonES p where p.age > 0 AND (p.personName = 'amit' OR p.personName = 'dev')";
        Query nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
        List resultList = nameQuery.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
    }

    /**
     * After class tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (node != null)
            node.close();
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        purge();
        em.close();
        emf.close();
    }

    /**
     * Wait thread.
     * 
     */
    private void waitThread()
    {
        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Verify each person object of result list.
     * 
     * @param resultPersonList
     *            the result person list
     * @param persons
     *            the persons
     */
    private void assertResultList(List<PersonES> resultPersonList, PersonES... persons)
    {
        boolean flag = false;

        Assert.assertNotNull(resultPersonList);
        Assert.assertEquals(persons.length, resultPersonList.size());

        for (PersonES person : persons)
        {
            flag = false;
            for (PersonES resultPerson : resultPersonList)
            {
                if (person.getPersonId().equals(resultPerson.getPersonId()))
                {
                    matchPerson(resultPerson, person);
                    flag = true;
                }
            }
            Assert.assertEquals("Person with id " + person.getPersonId() + " not found in Result list.", true, flag);
        }
    }

    /**
     * Match person to verify each field of both PersonES objects are same.
     * 
     * @param resultPerson
     *            the result person
     * @param person
     *            the person
     */
    private void matchPerson(PersonES resultPerson, PersonES person)
    {
        Assert.assertNotNull(resultPerson);
        Assert.assertEquals(person.getPersonId(), resultPerson.getPersonId());
        Assert.assertEquals(person.getPersonName(), resultPerson.getPersonName());
        Assert.assertEquals(person.getAge(), resultPerson.getAge());
        Assert.assertEquals(person.getSalary(), resultPerson.getSalary());
    }

    /**
     * Purge.
     */
    private void purge()
    {
        em.remove(em.find(PersonES.class, "1"));
        em.remove(em.find(PersonES.class, "2"));
        em.remove(em.find(PersonES.class, "3"));
        em.remove(em.find(PersonES.class, "4"));
        waitThread();
    }
}