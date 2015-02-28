/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
/*
 * author: karthikp.manchala
 */
package com.impetus.client.esindexer;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.esindexer.PersonESIndexerCassandra.Day;

// TODO: Auto-generated Javadoc
/**
 * The Class CassandraESAggregationTest.
 */
public class CassandraESAggregationTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The node. */
    private static Node node = null;

    /** The person. */
    private PersonESIndexerCassandra person;

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
            ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
            builder.put("path.data", "target/data");
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

        emf = Persistence.createEntityManagerFactory("esIndexerTest");
        em = emf.createEntityManager();
        init();
    }

    /**
     * Inits the.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    private void init() throws InterruptedException
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
        person = new PersonESIndexerCassandra();
        person.setAge(age);
        person.setDay(Day.FRIDAY);
        person.setPersonId(id);
        person.setPersonName(name);
        person.setSalary(salary);
        em.persist(person);
    }

    /**
     * Test min aggregation.
     */
    @Test
    public void testMinAggregation()
    {
        PersonESIndexerCassandra p = em.find(PersonESIndexerCassandra.class, "1");
        String queryString = "Select min(p.salary) from PersonESIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
    }

    /**
     * Test multi min aggregation.
     */
    @Test
    public void testMultiMinAggregation()
    {

        String queryString = "Select min(p.salary), min(p.age) from PersonESIndexerCassandra p where p.age > 20";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(300.0, resultList.get(0));
        Assert.assertEquals(30.0, resultList.get(1));
    }

    /**
     * Test max aggregation.
     */
    @Test
    public void testMaxAggregation()
    {
        String queryString = "Select max(p.age) from PersonESIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
    }

    /**
     * Test multi max aggregation.
     */
    @Test
    public void testMultiMaxAggregation()

    {
        String queryString = "Select max(p.age), max(p.salary) from PersonESIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    /**
     * Test sum aggregation.
     */
    @Test
    public void testSumAggregation()
    {
        String queryString = "Select sum(p.salary) from PersonESIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(1000.0, resultList.get(0));
    }

    /**
     * Test multi sum aggregation.
     */
    @Test
    public void testMultiSumAggregation()
    {
        String queryString = "Select sum(p.salary), sum(p.age) from PersonESIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(1000.0, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
    }

    /**
     * Test avg aggregation.
     */
    @Test
    public void testAvgAggregation()
    {
        String queryString = "Select avg(p.salary) from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(250.0, resultList.get(0));
    }

    /**
     * Test multi avg aggregation.
     */
    @Test
    public void testMultiAvgAggregation()
    {
        String avgQuery = "Select avg(p.salary), avg(p.age) from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(avgQuery);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(250.0, resultList.get(0));
        Assert.assertEquals(25.0, resultList.get(1));
    }

    /**
     * Test max min same field aggregation.
     */
    @Test
    public void testMaxMinSameFieldAggregation()
    {
        String queryString = "Select max(p.age), min(p.age) from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
        Assert.assertEquals(10.0, resultList.get(1));
    }

    /**
     * Test multi aggregation.
     */
    @Test
    public void testMultiAggregation()
    {
        String queryString = "Select min(p.age), min(p.salary), max(p.age), max(p.salary) from PersonESIndexerCassandraIndexerCassandra p";
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
    @Test
    public void testMinMaxSumAvgAggregation()
    {
        String queryString = "Select min(p.salary), max(p.salary), sum(p.salary), avg(p.salary) from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(4, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
        Assert.assertEquals(1000.0, resultList.get(2));
        Assert.assertEquals(250.0, resultList.get(3));
    }

    /**
     * Test attribute with min aggregation.
     */
    @Test
    public void testAttributeWithMinAggregation()
    {
        String queryString = "Select p.age, min(p.salary) from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
    }

    /**
     * Test min with attribute aggregation.
     */
    @Test
    public void testMinWithAttributeAggregation()
    {
        String queryString = "Select min(p.salary), p.age from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
        Assert.assertEquals(40, resultList.get(1));
    }

    /**
     * Test max with attribute aggregation.
     */
    @Test
    public void testMaxWithAttributeAggregation()
    {
        String queryString = "Select max(p.salary), p.salary from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(400.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    /**
     * Test attribute with sum aggregation.
     */
    @Test
    public void testAttributeWithSumAggregation()
    {
        String queryString = "Select p.age, sum(p.salary) from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40, resultList.get(0));
        Assert.assertEquals(1000.0, resultList.get(1));
    }

    /**
     * Test sum with attribute same field aggregation.
     */
    @Test
    public void testSumWithAttributeSameFieldAggregation()
    {
        String queryString = "Select sum(p.salary), p.salary from PersonESIndexerCassandraIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(1000.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    /**
     * Test attribute with avg aggregation.
     */
    @Test
    public void testAttributeWithAvgAggregation()
    {
        String queryString = "Select p.age, avg(p.salary) from PersonESIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(40, resultList.get(0));
        Assert.assertEquals(250.0, resultList.get(1));
    }

    /**
     * Test avg with attribute same field aggregation.
     */
    @Test
    public void testAvgWithAttributeSameFieldAggregation()
    {
        String queryString = "Select avg(p.salary), p.salary from PersonESIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
        Assert.assertEquals(250.0, resultList.get(0));
        Assert.assertEquals(400.0, resultList.get(1));
    }

    /**
     * Test multi field aggregation.
     */
    @Test
    public void testMultiFieldAggregation()
    {
        String queryString = "Select p.age, min(p.salary), p.salary, max(p.salary), p.personId, sum(p.salary), p.personName, avg(p.salary) from PersonESIndexerCassandra p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(8, resultList.size());
        Assert.assertEquals(40, resultList.get(0));
        Assert.assertEquals(100.0, resultList.get(1));
        Assert.assertEquals(400.0, resultList.get(2));
        Assert.assertEquals(400.0, resultList.get(3));
        Assert.assertEquals("4", resultList.get(4));
        Assert.assertEquals(1000.0, resultList.get(5));
        Assert.assertEquals("Pragalbh", resultList.get(6));
        Assert.assertEquals(250.0, resultList.get(7));
    }

    /**
     * Test aggregation with where clause.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Test
    public void testAggregationWithWhereClause() throws InterruptedException
    {
        waitThread();

        String invalidQueryWithAndClause = "Select min(p.age) from PersonESIndexerCassandra p where p.personName = 'amit' AND p.age = 34";
        Query nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
        List persons = nameQuery.getResultList();

        Assert.assertTrue(persons.isEmpty());

        String queryWithAndClause = "Select min(p.age) from PersonESIndexerCassandra p where p.personName = 'amit' AND p.age = 20";
        nameQuery = em.createNamedQuery(queryWithAndClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(20.0, persons.get(0));

        String queryWithORClause = "Select max(p.salary) from PersonESIndexerCassandra p where p.personName = 'amit' OR p.personName = 'dev'";
        nameQuery = em.createNamedQuery(queryWithORClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(200.0, persons.get(0));

        String queryMinMaxWithORClause = "Select max(p.salary), min(p.age) from PersonESIndexerCassandra p where p.personName = 'amit' OR p.personName = 'karthik'";
        nameQuery = em.createNamedQuery(queryMinMaxWithORClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(2, persons.size());
        Assert.assertEquals(300.0, persons.get(0));
        Assert.assertEquals(20.0, persons.get(1));

        String invalidQueryWithORClause = "Select sum(p.age) from PersonESIndexerCassandra p where p.personName = 'amit' OR p.personName = 'lkl'";
        nameQuery = em.createNamedQuery(invalidQueryWithORClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(20.0, persons.get(0));

        String testSumWithGreaterThanClause = "Select sum(p.age) from PersonESIndexerCassandra p where p.age > 15";
        nameQuery = em.createNamedQuery(testSumWithGreaterThanClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(90.0, persons.get(0));
        waitThread();
    }

    /**
     * Test positional query.
     */
    @Test
    public void testPositionalQuery()
    {
        String queryString = "Select min(p.salary) from PersonESIndexerCassandra p where p.personName = ?1";
        Query query = em.createQuery(queryString);
        query.setParameter(1, "amit");
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, resultList.get(0));
    }

    /**
     * Test parameter query.
     */
    @Test
    public void testParameterQuery()
    {
        String queryString = "Select p from PersonESIndexerCassandra p where p.personName = :personName";
        Query query = em.createQuery(queryString);
        query.setParameter("personName", "amit");
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(100.0, ((PersonESIndexerCassandra) resultList.get(0)).getSalary());
    }

    /**
     * Test between.
     */
    @Test
    public void testBetween()
    {
        String queryString = "Select p from PersonESIndexerCassandra p where p.age between 20 and 34";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
    }

    /**
     * Test between with expression.
     */
    @Test
    public void testBetweenWithExpression()
    {
        String queryString = "Select p from PersonESIndexerCassandra p where p.age between 15+5 and 40-6";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(2, resultList.size());
    }

    /**
     * Test between min.
     */
    @Test
    public void testBetweenMin()
    {
        String queryString = "Select min(p.age) from PersonESIndexerCassandra p where p.age between 18 and 34";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(20.0, resultList.get(0));
    }

    /**
     * Test like query.
     */
    @Test
    public void testLikeQuery()
    {
        String queryString = "Select min(p.age) from PersonESIndexerCassandra p where p.personName like '%mit'";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(20.0, resultList.get(0));
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
        if (node != null)
            node.close();
    }

    // @After
    /**
     * Tear down.
     */
    public void tearDown()
    {
        em.remove(em.find(PersonESIndexerCassandra.class, "1"));
        em.remove(em.find(PersonESIndexerCassandra.class, "2"));
        em.remove(em.find(PersonESIndexerCassandra.class, "3"));
        em.remove(em.find(PersonESIndexerCassandra.class, "4"));
        em.close();
        emf.close();
    }

    /**
     * Wait thread.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    private void waitThread() throws InterruptedException
    {
        Thread.sleep(2000);
    }
}