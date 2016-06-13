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
package com.impetus.kundera.client.query;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import junit.framework.Assert;

import com.impetus.kundera.query.Person;
import com.impetus.kundera.query.Person.Day;

/**
 * The Class GroupByBaseTest.
 * 
 * @author karthikp.manchala
 * 
 */
public abstract class GroupByBaseTest
{
    /** The emf. */
    protected static EntityManagerFactory emf;

    /** The em. */
    protected static EntityManager em;

    /** The person. */
    private static Person person;

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    protected static boolean checkIfServerRunning()
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
    protected static void init() throws InterruptedException
    {
        createPerson("1", 10, "Amit", 100.0);
        createPerson("2", 20, "Dev", 200.0);
        createPerson("3", 30, "Karthik", 300.0);
        createPerson("4", 40, "Pragalbh", 400.0);
        createPerson("5", 10, "AK", 500.0);
        createPerson("6", 20, "D", 600.0);
        createPerson("7", 30, "KPM", 700.0);
        createPerson("8", 40, "PG", 800.0);
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
     */
    private static void createPerson(String id, int age, String name, Double salary)
    {
        person = new Person();
        person.setAge(age);
        person.setPersonId(id);
        person.setPersonName(name);
        person.setSalary(salary);
//        person.setDay(Day.FRIDAY);
        em.persist(person);
    }

    /**
     * Test aggregation.
     */
    protected void testAggregation()
    {
        testGroupBy();
        testGroupByRowKey();
        testGroupByWithWhereClause();
        testGroupByWithEntity();
        testGroupByWithFields();
        testGroupByWithMetricAgg();

        testHaving();
        testHavingWithWhereClause();
        testHavingWithEntity();
        testHavingWithCount();
        testHavingWithNoMatch();
        testHavingWithMetricAgg();

        testHavingWithAnd();
        testHavingWithOr();

        // testGroupByBuckets();
    }

    /**
     * Test group by.
     */
    private void testGroupBy()
    {
        String queryString = "Select sum(p.salary) from Person p group by p.age";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        Assert.assertEquals(600.0, (resultList.get(0)));
        Assert.assertEquals(800.0, (resultList.get(1)));
        Assert.assertEquals(1000.0, (resultList.get(2)));
        Assert.assertEquals(1200.0, (resultList.get(3)));
    }

    /**
     * Test group by row key.
     */
    private void testGroupByRowKey()
    {
        String queryString = "Select sum(p.salary) from Person p group by p.personId";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(8, resultList.size());
        Assert.assertEquals(100.0, (resultList.get(0)));
        Assert.assertEquals(200.0, (resultList.get(1)));
        Assert.assertEquals(300.0, (resultList.get(2)));
        Assert.assertEquals(400.0, (resultList.get(3)));
        Assert.assertEquals(500.0, (resultList.get(4)));
        Assert.assertEquals(600.0, (resultList.get(5)));
        Assert.assertEquals(700.0, (resultList.get(6)));
        Assert.assertEquals(800.0, (resultList.get(7)));
    }

    /**
     * Test group by with where clause.
     */
    private void testGroupByWithWhereClause()
    {
        String queryString = "Select sum(p.salary) from Person p where p.age > 20 group by p.age";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(1000.0, (resultList.get(0)));
        Assert.assertEquals(1200.0, (resultList.get(1)));
    }

    /**
     * Test group by with entity.
     */
    private void testGroupByWithEntity()
    {
        String queryString = "Select p from Person p where p.age < 20 group by p.age";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Person person = (Person) (resultList.get(0));
        Assert.assertEquals("5", person.getPersonId());
        Assert.assertEquals("AK", person.getPersonName());
        Assert.assertEquals(10, person.getAge().intValue());
        Assert.assertEquals(500.0, person.getSalary());
    }

    /**
     * Test group by with fields.
     */
    private void testGroupByWithFields()
    {
        String queryString = "Select p.age, p.personName, p.personId from Person p where p.age < 30 group by p.age";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());

        List result = (List) resultList.get(0);
        Assert.assertEquals(10, result.get(0));

        result = (List) resultList.get(1);
        Assert.assertEquals(20, result.get(0));
    }

    /**
     * Test group by with metric agg.
     */
    private void testGroupByWithMetricAgg()
    {
        String queryString = "Select p.salary, sum(p.age), p.personName, p.personId from Person p where p.age < 30 group by p.salary";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        List result = (List) resultList.get(0);
        Assert.assertEquals(100.0, result.get(0));
        Assert.assertEquals(10.0, result.get(1));
        Assert.assertEquals("Amit", result.get(2));
        Assert.assertEquals("1", result.get(3));

        result = (List) resultList.get(1);
        Assert.assertEquals(200.0, result.get(0));
        Assert.assertEquals(20.0, result.get(1));
        Assert.assertEquals("Dev", result.get(2));
        Assert.assertEquals("2", result.get(3));

        result = (List) resultList.get(2);
        Assert.assertEquals(500.0, result.get(0));
        Assert.assertEquals(10.0, result.get(1));
        Assert.assertEquals("AK", result.get(2));
        Assert.assertEquals("5", result.get(3));

        result = (List) resultList.get(3);
        Assert.assertEquals(600.0, result.get(0));
        Assert.assertEquals(20.0, result.get(1));
        Assert.assertEquals("D", result.get(2));
        Assert.assertEquals("6", result.get(3));
    }

    /**
     * Test having.
     */
    private void testHaving()
    {
        String queryString = "Select sum(p.salary) from Person p group by p.age having avg(p.age) > 20";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());

        Assert.assertEquals(1000.0, (resultList.get(0)));
        Assert.assertEquals(1200.0, (resultList.get(1)));
    }

    /**
     * Test having with where clause.
     */
    private void testHavingWithWhereClause()
    {
        String queryString = "Select sum(p.salary) from Person p where p.age > 20 group by p.age having sum(p.age) > 70";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(1200.0, (resultList.get(0)));
    }

    /**
     * Test having with entity.
     */
    private void testHavingWithEntity()
    {
        String queryString = "Select p from Person p where p.age < 40 group by p.age having max(p.salary) > 600";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Person person = (Person) (resultList.get(0));
        Assert.assertEquals("7", person.getPersonId());
        Assert.assertEquals("KPM", person.getPersonName());
        Assert.assertEquals(30, person.getAge().intValue());
        Assert.assertEquals(700.0, person.getSalary());
    }

    /**
     * Test having with count.
     */
    private void testHavingWithCount()
    {
        String queryString = "Select p.age, p.personName, p.personId from Person p where p.age < 30 group by p.age having count(p) > 1";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());

        List result = (List) resultList.get(0);
        Assert.assertEquals(10, result.get(0));

        result = (List) resultList.get(1);
        Assert.assertEquals(20, result.get(0));
    }

    /**
     * Test having with no match.
     */
    private void testHavingWithNoMatch()
    {
        String queryString = "Select p.age, p.personName, p.personId from Person p where p.age < 30 group by p.age having count(p) > 2";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(0, resultList.size());
    }

    /**
     * Test having with metric agg.
     */
    private void testHavingWithMetricAgg()
    {
        String queryString = "Select p.salary, sum(p.age), p.personName, p.personId from Person p group by p.personId having sum(p.age) < 30";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        List result = (List) resultList.get(0);
        Assert.assertEquals(100.0, result.get(0));
        Assert.assertEquals(10.0, result.get(1));
        Assert.assertEquals("Amit", result.get(2));
        Assert.assertEquals("1", result.get(3));

        result = (List) resultList.get(1);
        Assert.assertEquals(200.0, result.get(0));
        Assert.assertEquals(20.0, result.get(1));
        Assert.assertEquals("Dev", result.get(2));
        Assert.assertEquals("2", result.get(3));

        result = (List) resultList.get(2);
        Assert.assertEquals(500.0, result.get(0));
        Assert.assertEquals(10.0, result.get(1));
        Assert.assertEquals("AK", result.get(2));
        Assert.assertEquals("5", result.get(3));

        result = (List) resultList.get(3);
        Assert.assertEquals(600.0, result.get(0));
        Assert.assertEquals(20.0, result.get(1));
        Assert.assertEquals("D", result.get(2));
        Assert.assertEquals("6", result.get(3));
    }

    /**
     * Test having with and.
     */
    private void testHavingWithAnd()
    {
        String queryString = "Select p.age, p.personName, p.personId from Person p where p.age > 20 group by p.age having sum(p.age) > 50 and avg(p.age) < 40";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());

        List result = (List) resultList.get(0);
        Assert.assertEquals(30, result.get(0));
        Assert.assertEquals("KPM", result.get(1));
        Assert.assertEquals("7", result.get(2));
    }

    /**
     * Test having with or.
     */
    private void testHavingWithOr()
    {
        String queryString = "Select sum(p.age), count(p.age) from Person p where p.salary > 200.0 group by p.age having sum(p.age) > 50 or avg(p.age) < 40";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());

        List result = (List) resultList.get(0);
        Assert.assertEquals(60.0, result.get(0));
        Assert.assertEquals(2.0, result.get(1));

        result = (List) resultList.get(1);
        Assert.assertEquals(80.0, result.get(0));
        Assert.assertEquals(2.0, result.get(1));

        result = (List) resultList.get(2);
        Assert.assertEquals(10.0, result.get(0));
        Assert.assertEquals(1.0, result.get(1));

        result = (List) resultList.get(3);
        Assert.assertEquals(20.0, result.get(0));
        Assert.assertEquals(1.0, result.get(1));
    }

    /**
     * Test group by buckets.
     */
    private void testGroupByBuckets()
    {
        createPerson("9", 10, "Amit", 100.0);
        createPerson("10", 10, "Dev", 200.0);
        createPerson("11", 10, "Karthik", 300.0);
        createPerson("12", 10, "Pragalbh", 400.0);
        createPerson("13", 10, "AK", 500.0);
        createPerson("14", 10, "D", 600.0);
        createPerson("15", 10, "KPM", 700.0);
        createPerson("16", 10, "PG", 800.0);
        try
        {
            waitThread();
        }
        catch (InterruptedException e)
        {
        }

        String queryString = "Select sum(p.age) from Person p where p.age < 40 group by p.personId";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(14, resultList.size());

        queryString = "Select sum(p.age) from Person p where p.age < 40 group by p.age";
        query = em.createQuery(queryString);
        resultList = query.getResultList();

        Assert.assertEquals(3, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
    }

    /**
     * Wait thread.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    protected static void waitThread() throws InterruptedException
    {
        Thread.sleep(2000);
    }
}