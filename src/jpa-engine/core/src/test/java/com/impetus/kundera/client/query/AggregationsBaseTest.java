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
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.query.Person;
import com.impetus.kundera.query.Person.Day;

/**
 * The Class AggregationsBaseTest.
 * 
 * @author: karthikp.manchala
 * 
 */
public abstract class AggregationsBaseTest
{

    /** The emf. */
    protected EntityManagerFactory emf;

    /** The em. */
    protected EntityManager em;

    /** The person. */
    private Person person;

    /**
     * logger
     */
    private Logger logger = LoggerFactory.getLogger(AggregationsBaseTest.class);

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
    protected void init() throws InterruptedException
    {

        createPerson("1", 20, "Amit", 100.0);
        createPerson("2", 10, "Dev", 200.0);
        createPerson("3", 30, "Karthik", 300.0);
        createPerson("4", 40, "Pragalbh", 400.0);
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
    private void createPerson(String id, int age, String name, Double salary)
    {
        person = new Person();
        person.setAge(age);
//        person.setDay(Day.FRIDAY);
        person.setPersonId(id);
        person.setPersonName(name);
        person.setSalary(salary);
        em.persist(person);
    }

    /**
     * Test min aggregation.
     */
    protected void testAggregation()
    {
        testMinAggregation();
        testMultiMinAggregation();
        testMaxAggregation();
        testMultiMaxAggregation();
        testSumAggregation();
        testMultiSumAggregation();
        testAvgAggregation();
        testMultiAvgAggregation();
        testMaxMinSameFieldAggregation();
        testMultiAggregation();
        testMinMaxSumAvgAggregation();
        testAggregationWithWhereClause();
        testPositionalQuery();
        testParameterQuery();
        testBetween();
        testBetweenWithExpression();
        testBetweenMin();
        testLikeQuery();

    }

    /**
     * Test min aggregation.
     */
    private void testMinAggregation()
    {
        Person p = em.find(Person.class, "1");
        String queryString = "Select min(p.salary) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
    }

    /**
     * Test multi min aggregation.
     */
    private void testMultiMinAggregation()
    {

        String queryString = "Select min(p.salary), min(p.age) from Person p where p.age > 20";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(300.0, resultList.get(0));
        Assert.assertEquals(30.0, resultList.get(1));
    }

    /**
     * Test max aggregation.
     */
    private void testMaxAggregation()
    {
        String queryString = "Select max(p.age) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
    }

    /**
     * Test multi max aggregation.
     */
    private void testMultiMaxAggregation()

    {
        String queryString = "Select max(p.age), max(p.salary) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    /**
     * Test sum aggregation.
     */
    private void testSumAggregation()
    {
        String queryString = "Select sum(p.salary) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(1000.0, resultList.get(0));
    }

    /**
     * Test multi sum aggregation.
     */
    private void testMultiSumAggregation()
    {
        String queryString = "Select sum(p.salary), sum(p.age) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(1000.0, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
    }

    /**
     * Test avg aggregation.
     */
    private void testAvgAggregation()
    {
        String queryString = "Select avg(p.salary) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(250.0, resultList.get(0));
    }

    /**
     * Test multi avg aggregation.
     */
    private void testMultiAvgAggregation()
    {
        String avgQuery = "Select avg(p.salary), avg(p.age) from Person p";
        Query query = em.createQuery(avgQuery);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(250.0, resultList.get(0));
        Assert.assertEquals(25.0, resultList.get(1));
    }

    /**
     * Test max min same field aggregation.
     */
    private void testMaxMinSameFieldAggregation()
    {
        String queryString = "Select max(p.age), min(p.age) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
        Assert.assertEquals(10.0, resultList.get(1));
    }

    /**
     * Test multi aggregation.
     */
    private void testMultiAggregation()
    {
        String queryString = "Select min(p.age), min(p.salary), max(p.age), max(p.salary) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());
        Assert.assertEquals(10.0, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
        Assert.assertEquals(40.0, resultList.get(2));
        Assert.assertEquals(400.0, resultList.get(3));
    }

    /**
     * Test min max sum avg aggregation.
     */
    private void testMinMaxSumAvgAggregation()
    {
        String queryString = "Select min(p.salary), max(p.salary), sum(p.salary), avg(p.salary) from Person p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
        Assert.assertEquals(1000.0, resultList.get(2));
        Assert.assertEquals(250.0, resultList.get(3));
    }

    /**
     * Test aggregation with where clause.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    private void testAggregationWithWhereClause()
    {
        try
        {
            waitThread();

            String invalidQueryWithAndClause = "Select min(p.age) from Person p where p.personName = 'amit' AND p.age = 34";
            Query nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
            List persons = nameQuery.getResultList();

            Assert.assertTrue(persons.isEmpty());

            String queryWithAndClause = "Select min(p.age) from Person p where p.personName = 'amit' AND p.age = 20";
            nameQuery = em.createNamedQuery(queryWithAndClause);
            persons = nameQuery.getResultList();

            Assert.assertFalse(persons.isEmpty());
            Assert.assertEquals(1, persons.size());
            Assert.assertEquals(20.0, persons.get(0));

            String queryWithORClause = "Select max(p.salary) from Person p where p.personName = 'amit' OR p.personName = 'dev'";
            nameQuery = em.createNamedQuery(queryWithORClause);
            persons = nameQuery.getResultList();

            Assert.assertFalse(persons.isEmpty());
            Assert.assertEquals(1, persons.size());
            Assert.assertEquals(200.0, persons.get(0));

            String queryMinMaxWithORClause = "Select max(p.salary), min(p.age) from Person p where p.personName = 'amit' OR p.personName = 'karthik'";
            nameQuery = em.createNamedQuery(queryMinMaxWithORClause);
            persons = nameQuery.getResultList();

            Assert.assertFalse(persons.isEmpty());
            Assert.assertEquals(2, persons.size());
            Assert.assertEquals(300.0, persons.get(0));
            Assert.assertEquals(20.0, persons.get(1));

            String invalidQueryWithORClause = "Select sum(p.age) from Person p where p.personName = 'amit' OR p.personName = 'lkl'";
            nameQuery = em.createNamedQuery(invalidQueryWithORClause);
            persons = nameQuery.getResultList();

            Assert.assertFalse(persons.isEmpty());
            Assert.assertEquals(1, persons.size());
            Assert.assertEquals(20.0, persons.get(0));

            String testSumWithGreaterThanClause = "Select sum(p.age) from Person p where p.age > 15";
            nameQuery = em.createNamedQuery(testSumWithGreaterThanClause);
            persons = nameQuery.getResultList();

            Assert.assertFalse(persons.isEmpty());
            Assert.assertEquals(1, persons.size());
            Assert.assertEquals(90.0, persons.get(0));
            waitThread();
        }
        catch (InterruptedException e)
        {
            logger.error(e.getMessage());
        }
    }

    /**
     * Test positional query.
     */
    private void testPositionalQuery()
    {
        String queryString = "Select min(p.salary) from Person p where p.personName = ?1";
        Query query = em.createQuery(queryString);
        query.setParameter(1, "amit");
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
    }

    /**
     * Test parameter query.
     */
    private void testParameterQuery()
    {
        String queryString = "Select p from Person p where p.personName = :personName";
        Query query = em.createQuery(queryString);
        query.setParameter("personName", "amit");
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, ((Person) resultList.get(0)).getSalary());
    }

    /**
     * Test between.
     */
    private void testBetween()
    {
        String queryString = "Select p from Person p where p.age between 20 and 34";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
    }

    /**
     * Test between with expression.
     */
    protected void testBetweenWithExpression()
    {
        String queryString = "Select p from Person p where p.age between 15+5 and 40-6";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
    }

    /**
     * Test between min.
     */
    private void testBetweenMin()
    {
        String queryString = "Select min(p.age) from Person p where p.age between 18 and 34";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(20.0, resultList.get(0));
    }

    /**
     * Test like query.
     */
    private void testLikeQuery()
    {
        String queryString = "Select min(p.age) from Person p where p.personName like '%mit'";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(20.0, resultList.get(0));
    }

    /**
     * Wait thread.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    protected void waitThread() throws InterruptedException
    {
        Thread.sleep(2000);
    }
}