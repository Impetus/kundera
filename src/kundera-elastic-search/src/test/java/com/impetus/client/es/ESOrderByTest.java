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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.es.PersonES.Day;

/**
 * The Class ESGroupByTest.
 * 
 * @author Amit Kumar
 */
public class ESOrderByTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /** The node. */
    private static Node node = null;

    /** The person. */
    private static PersonES persons[] = new PersonES[4];

    /**
     * Sets the up before class.
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

        emf = Persistence.createEntityManagerFactory("es-pu");
        em = emf.createEntityManager();
        init();
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
     * Inits the.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    private static void init() throws InterruptedException
    {
        persons[0] = createPerson("1", 50, "karthik", 500.0);
        persons[1] = createPerson("2", 20, "pragalbh", 700.0);
        persons[2] = createPerson("3", 60, "amit", 300.0);
        persons[3] = createPerson("4", 40, "dev", 400.0);

        waitThread();
    }

    /**
     * Creates the person.
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
    private static PersonES createPerson(String id, int age, String name, Double salary)
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
     * Test query.
     */
    @Test
    public void testQuery()
    {
        testOrderBy();
        testOrderByDescending();
        testOrderByRowId();
        testOrderByRowIdDescending();
        testOrderByAscending();
        testOrderByDescWithString();
        testOrderByASCWhereClause();
        testOrderByASCMultipleFields();
        testOrderByWithGroupBy();
        testOrderByWithGroupByDESC();
        testOrderByWithGroupByString();
        testOrderByWithGroupByStringDESC();
    }

    /**
     * Test order by.
     */
    private void testOrderBy()
    {
        String queryString = "Select p from PersonES p order by p.age";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        assertPerson((PersonES) resultList.get(0), persons[1]);
        assertPerson((PersonES) resultList.get(1), persons[3]);
        assertPerson((PersonES) resultList.get(2), persons[0]);
        assertPerson((PersonES) resultList.get(3), persons[2]);
    }

    /**
     * Test order by descending.
     */
    private void testOrderByDescending()
    {
        String queryString = "Select p from PersonES p order by p.age DESC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        assertPerson((PersonES) resultList.get(0), persons[2]);
        assertPerson((PersonES) resultList.get(1), persons[0]);
        assertPerson((PersonES) resultList.get(2), persons[3]);
        assertPerson((PersonES) resultList.get(3), persons[1]);
    }

    /**
     * Test order by row id.
     */
    private void testOrderByRowId()
    {
        String queryString = "Select p from PersonES p order by p.personId";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        assertPerson((PersonES) resultList.get(0), persons[0]);
        assertPerson((PersonES) resultList.get(1), persons[1]);
        assertPerson((PersonES) resultList.get(2), persons[2]);
        assertPerson((PersonES) resultList.get(3), persons[3]);
    }

    /**
     * Test order by row id descending.
     */
    private void testOrderByRowIdDescending()
    {
        String queryString = "Select p from PersonES p order by p.personId DESC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        assertPerson((PersonES) resultList.get(0), persons[3]);
        assertPerson((PersonES) resultList.get(1), persons[2]);
        assertPerson((PersonES) resultList.get(2), persons[1]);
        assertPerson((PersonES) resultList.get(3), persons[0]);
    }

    /**
     * Test order by ascending.
     */
    private void testOrderByAscending()
    {
        String queryString = "Select p.personName from PersonES p order by p.age ASC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        Assert.assertEquals("pragalbh", resultList.get(0));
        Assert.assertEquals("dev", resultList.get(1));
        Assert.assertEquals("karthik", resultList.get(2));
        Assert.assertEquals("amit", resultList.get(3));
    }

    /**
     * Test order by desc with string.
     */
    private void testOrderByDescWithString()
    {
        String queryString = "Select p.personName from PersonES p order by p.personName DESC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        Assert.assertEquals("pragalbh", resultList.get(0));
        Assert.assertEquals("karthik", resultList.get(1));
        Assert.assertEquals("dev", resultList.get(2));
        Assert.assertEquals("amit", resultList.get(3));
    }

    /**
     * Test order by asc where clause.
     */
    private void testOrderByASCWhereClause()
    {
        String queryString = "Select p.personName from PersonES p where p.salary > 350.0 order by p.age ASC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(3, resultList.size());

        Assert.assertEquals("pragalbh", resultList.get(0));
        Assert.assertEquals("dev", resultList.get(1));
        Assert.assertEquals("karthik", resultList.get(2));
    }

    /**
     * Test order by asc multiple fields.
     */
    private void testOrderByASCMultipleFields()
    {
        String queryString = "Select p.personName, p.age, p.salary from PersonES p where p.salary <> 300.0 order by p.age ASC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(3, resultList.size());

        Assert.assertEquals("pragalbh", ((List) resultList.get(0)).get(0));
        Assert.assertEquals(20, ((List) resultList.get(0)).get(1));
        Assert.assertEquals(700.0, ((List) resultList.get(0)).get(2));

        Assert.assertEquals("dev", ((List) resultList.get(1)).get(0));
        Assert.assertEquals(40, ((List) resultList.get(1)).get(1));
        Assert.assertEquals(400.0, ((List) resultList.get(1)).get(2));

        Assert.assertEquals("karthik", ((List) resultList.get(2)).get(0));
        Assert.assertEquals(50, ((List) resultList.get(2)).get(1));
        Assert.assertEquals(500.0, ((List) resultList.get(2)).get(2));
    }

    /**
     * Test order by with group by.
     */
    private void testOrderByWithGroupBy()
    {
        String queryString = "Select p from PersonES p where p.salary <> 300.0 group by p.age order by p.age ASC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(3, resultList.size());

        assertPerson((PersonES) resultList.get(0), persons[1]);
        assertPerson((PersonES) resultList.get(1), persons[3]);
        assertPerson((PersonES) resultList.get(2), persons[0]);
    }

    /**
     * Test order by with group by desc.
     */
    private void testOrderByWithGroupByDESC()
    {
        String queryString = "Select p from PersonES p group by p.age order by p.age DESC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        assertPerson((PersonES) resultList.get(0), persons[2]);
        assertPerson((PersonES) resultList.get(1), persons[0]);
        assertPerson((PersonES) resultList.get(2), persons[3]);
        assertPerson((PersonES) resultList.get(3), persons[1]);
    }

    /**
     * Test order by with group by string.
     */
    private void testOrderByWithGroupByString()
    {
        String queryString = "Select p.personName, p.age, p.salary from PersonES p where p.salary <> 300.0 group by p.personName order by p.personName ASC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(3, resultList.size());

        Assert.assertEquals("dev", ((List) resultList.get(0)).get(0));
        Assert.assertEquals(40, ((List) resultList.get(0)).get(1));
        Assert.assertEquals(400.0, ((List) resultList.get(0)).get(2));

        Assert.assertEquals("karthik", ((List) resultList.get(1)).get(0));
        Assert.assertEquals(50, ((List) resultList.get(1)).get(1));
        Assert.assertEquals(500.0, ((List) resultList.get(1)).get(2));

        Assert.assertEquals("pragalbh", ((List) resultList.get(2)).get(0));
        Assert.assertEquals(20, ((List) resultList.get(2)).get(1));
        Assert.assertEquals(700.0, ((List) resultList.get(2)).get(2));
    }

    /**
     * Test order by with group by string desc.
     */
    private void testOrderByWithGroupByStringDESC()
    {
        String queryString = "Select p.personName, p.age, p.salary from PersonES p where p.salary <> 300.0 group by p.personName order by p.personName DESC";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(3, resultList.size());

        Assert.assertEquals("pragalbh", ((List) resultList.get(0)).get(0));
        Assert.assertEquals(20, ((List) resultList.get(0)).get(1));
        Assert.assertEquals(700.0, ((List) resultList.get(0)).get(2));

        Assert.assertEquals("karthik", ((List) resultList.get(1)).get(0));
        Assert.assertEquals(50, ((List) resultList.get(1)).get(1));
        Assert.assertEquals(500.0, ((List) resultList.get(1)).get(2));

        Assert.assertEquals("dev", ((List) resultList.get(2)).get(0));
        Assert.assertEquals(40, ((List) resultList.get(2)).get(1));
        Assert.assertEquals(400.0, ((List) resultList.get(2)).get(2));
    }

    /**
     * Assert person.
     * 
     * @param actual
     *            the actual
     * @param person
     *            the person
     */
    private void assertPerson(Object actual, PersonES person)
    {
        PersonES actualPerson = (PersonES) actual;

        Assert.assertEquals(actualPerson.getPersonId(), person.getPersonId());
        Assert.assertEquals(actualPerson.getPersonName(), person.getPersonName());
        Assert.assertEquals(actualPerson.getAge().intValue(), person.getAge().intValue());
        Assert.assertEquals(actualPerson.getSalary(), person.getSalary());
        Assert.assertEquals(actualPerson.getDay(), person.getDay());
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
        em.createQuery("delete from PersonES p").executeUpdate();
        waitThread();

        em.close();
        emf.close();

        if (node != null)
            node.close();
    }

    /**
     * Wait thread.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    private static void waitThread() throws InterruptedException
    {
        Thread.sleep(2000);
    }
}
