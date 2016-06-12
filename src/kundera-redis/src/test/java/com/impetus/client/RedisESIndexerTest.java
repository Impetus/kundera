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
package com.impetus.client;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.entities.Month;
import com.impetus.client.entities.PersonRedis;
import com.impetus.client.entities.PersonRedis.Day;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

import junit.framework.Assert;

/**
 * The Class RedisESIndexerTest.
 * 
 * @author Amit Kumar
 */
public class RedisESIndexerTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redisElasticSearch_pu";

    /** The Constant ROW_KEY. */
    private static final String ROW_KEY = "1";

    /** The node. */
    private static Node node = null;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisESIndexerTest.class);

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Builder builder = Settings.settingsBuilder();
        builder.put("path.home", "target/data");
        node = new NodeBuilder().settings(builder).node();
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
        node.close();
        LuceneCleanupUtilities.cleanDir("target/data");
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

        emf = Persistence.createEntityManagerFactory(REDIS_PU);
        em = emf.createEntityManager();
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

        purge();
        emf.close();
    }

    /**
     * Crud test with es.
     */
    @Test
    public void crudTestWithES()
    {

        logger.info("Crud tests for ES");
        PersonRedis person2 = preparePerson("Amit", "2", 40);

        // Persist records
        em.persist(person2);

        waitThread(10);

        PersonRedis fetchPerson = em.find(PersonRedis.class, "2");
        // Assertion for fetching objects
        Assert.assertEquals("2", fetchPerson.getPersonId());
        Assert.assertEquals("Amit", fetchPerson.getPersonName());
        Assert.assertEquals(40, fetchPerson.getAge().intValue());

        fetchPerson.setAge(50);
        em.merge(fetchPerson);

        fetchPerson = em.find(PersonRedis.class, "2");
        // Assertion for merge
        Assert.assertEquals("2", fetchPerson.getPersonId());
        Assert.assertEquals("Amit", fetchPerson.getPersonName());
        Assert.assertEquals(50, fetchPerson.getAge().intValue());

        em.remove(fetchPerson);
        // Assertion for remove
        fetchPerson = null;
        fetchPerson = em.find(PersonRedis.class, "2");
        Assert.assertNull(fetchPerson);

    }

    /**
     * Test like.
     */
    @Test
    // Need to enable the support
    public void testLike()
    {
        PersonRedis person = preparePerson("Amit", ROW_KEY, 32);
        em.persist(person);
        person = preparePerson("Devender", ROW_KEY + 1, 34);
        em.persist(person);
        waitThread(5);

        Query query;
        List<PersonRedis> results;
        // find by key.
        String findById = "Select p from PersonRedis p where p.personName like 'mit'";
        query = em.createQuery(findById);
        // query.setParameter("personId", ROW_KEY);
        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Amit", results.get(0).getPersonName());
    }

    /**
     * Query testwith es.
     */
    @Test
    public void QueryTestwithES()
    {

        logger.info("Query tests for ES");
        Query query;
        List<PersonRedis> results;

        // persist record.
        PersonRedis person = preparePerson("Amit", ROW_KEY, 32);
        em.persist(person);
        person = preparePerson("Amit", ROW_KEY + 1, 34);
        em.persist(person);
        person = preparePerson("Amit", ROW_KEY + 3, 29);
        em.persist(person);

        waitThread(10);

        testFindWithOutWhereClause();

        testFindById();

        testFindByNonRowKey();

        testFindByRowAndNonRowKey();

        testFindByBetweenOverNonRowKey();

        testFindByBetweenOverRowKey();

        testFindByGTELTEClauseNonRowKey();

        testFindParticularColumn();

        testFindByIdGTELTE();

        testInvalidDifferentClause();

        testSelectSpecificColumn();

        testORClause();

        testMultipleORClause();

        testDelete();
    }

    @Test
    public void indexDeletionTest() throws Exception
    {
        // persist record.
        em.persist(preparePerson("Amit", "1", 20));
        em.persist(preparePerson("Amit", "2", 30));
        em.persist(preparePerson("Amit", "3", 40));
        em.persist(preparePerson("Amit", "4", 50));
        Thread.sleep(1000);

        String query = "Select min(p.age) from PersonRedis p";
        List resultList = em.createQuery(query).getResultList();
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(20.0, resultList.get(0));

        PersonRedis person = em.find(PersonRedis.class, "1");
        em.remove(person);
        Thread.sleep(1000);
        query = "Select min(p.age) from PersonRedis p";
        resultList = em.createQuery(query).getResultList();
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(30.0, resultList.get(0));

        person = em.find(PersonRedis.class, "2");
        em.remove(person);
        Thread.sleep(1000);
        query = "Select min(p.age) from PersonRedis p";
        resultList = em.createQuery(query).getResultList();
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(40.0, resultList.get(0));
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        Query query;
        List<PersonRedis> results;

        // Delete by query.
        String deleteQuery = "Delete from PersonRedis p";
        query = em.createQuery(deleteQuery);
        int updateCount = query.executeUpdate();
        Assert.assertEquals(3, updateCount);

        // Search all after delete.
        String findWithOutWhereClause = "Select p from PersonRedis p";
        query = em.createQuery(findWithOutWhereClause);
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertTrue(results.size() == 0);
    }

    /**
     * Test multiple or clause.
     */
    private void testMultipleORClause()
    {
        Query query;
        List<PersonRedis> results;
        String findByIdMoreOrAge = "Select p from PersonRedis p where p.personId=:personId OR p.age=:age OR p.personName=:personName";
        query = em.createQuery(findByIdMoreOrAge);
        query.setParameter("personId", ROW_KEY);
        query.setParameter("age", 29);
        query.setParameter("personName", "amit");

        results = query.getResultList();
        Assert.assertEquals(3, results.size());
    }

    /**
     * Test or clause.
     */
    private void testORClause()
    {
        Query query;
        List<PersonRedis> results;
        // Find by key and now row key
        String findByIdOrAge = "Select p from PersonRedis p where p.personId=:personId OR p.age=:age";
        query = em.createQuery(findByIdOrAge);
        query.setParameter("personId", ROW_KEY);
        query.setParameter("age", 29);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        boolean isPresent = false;
        for (PersonRedis r : results)
        {
            if (r.getAge().equals(29) && !r.getPersonId().equals(ROW_KEY))
            {
                isPresent = true;
                break;
            }
        }

        Assert.assertTrue(isPresent);
    }

    /**
     * Test select specific column.
     */
    private void testSelectSpecificColumn()
    {
        Query query;
        List<PersonRedis> results;
        String findSelective = "Select p.age from PersonRedis p";
        query = em.createQuery(findSelective);
        results = query.getResultList();
        Assert.assertEquals(3, results.size());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());
    }

    /**
     * Test invalid different clause.
     */
    private void testInvalidDifferentClause()
    {
        Query query;
        String invalidDifferentClause = "Select p from PersonRedis p where p.personId=:personId AND p.age >=:age";
        query = em.createQuery(invalidDifferentClause);
        query.setParameter("personId", ROW_KEY);
        query.setParameter("age", 32);
        List<PersonRedis> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNotNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());

    }

    /**
     * Test find particular column.
     */
    private void testFindParticularColumn()
    {
        Query query;
        List<PersonRedis> results;
        String q = "select p.personId from PersonRedis p";
        query = em.createQuery(q);
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());
    }

    /**
     * Find by greater than and less than clause over non row key.
     */
    private void testFindByGTELTEClauseNonRowKey()
    {
        Query query;
        List<PersonRedis> results;
        // Find by greater than and less than clause over non row key
        String findAgeByGTELTEClause = "Select p from PersonRedis p where p.age <=:max AND p.age>=:min";
        query = em.createQuery(findAgeByGTELTEClause);
        query.setParameter("min", 32);
        query.setParameter("max", 35);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("Amit", results.get(0).getPersonName());
    }

    /**
     * Between clause over rowkey.
     */
    private void testFindByBetweenOverRowKey()
    {
        Query query;
        List<PersonRedis> results;
        // Between clause over rowkey
        String findIdByBetween = "Select p from PersonRedis p where p.personId between :min AND :max";
        query = em.createQuery(findIdByBetween);
        query.setParameter("min", ROW_KEY);
        query.setParameter("max", ROW_KEY + 1);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("Amit", results.get(0).getPersonName());
    }

    /**
     * find by between over non rowkey.
     */
    private void testFindByBetweenOverNonRowKey()
    {
        Query query;
        List<PersonRedis> results;
        // find by between over non rowkey
        String findAgeByBetween = "Select p from PersonRedis p where p.age between :min AND :max";
        query = em.createQuery(findAgeByBetween);
        query.setParameter("min", 32);
        query.setParameter("max", 35);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("Amit", results.get(0).getPersonName());
    }

    /**
     * Test find by row and non row key.
     */
    private void testFindByRowAndNonRowKey()
    {
        Query query;
        List<PersonRedis> results;
        // Find by key and now row key
        String findByIdAndAge = "Select p from PersonRedis p where p.personId=:personId AND p.age=:age";
        query = em.createQuery(findByIdAndAge);
        query.setParameter("personId", ROW_KEY);
        query.setParameter("age", 32);

        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Amit", results.get(0).getPersonName());
    }

    /**
     * Find by now row key.
     */
    private void testFindByNonRowKey()
    {
        Query query;
        List<PersonRedis> results;
        // Find by now row key
        String findByAge = "Select p from PersonRedis p where p.age=:age";
        query = em.createQuery(findByAge);
        query.setParameter("age", 32);

        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Amit", results.get(0).getPersonName());
        Assert.assertEquals(ROW_KEY, results.get(0).getPersonId());
    }

    /**
     * find by key.
     */
    private void testFindById()
    {
        Query query;
        List<PersonRedis> results;
        // find by key.
        String findById = "Select p from PersonRedis p where p.personId=:personId";
        query = em.createQuery(findById);
        query.setParameter("personId", ROW_KEY);
        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("Amit", results.get(0).getPersonName());
    }

    /**
     * Find without where clause.
     */
    private void testFindWithOutWhereClause()
    {
        // Find without where clause.
        String findWithOutWhereClause = "Select p from PersonRedis p";
        Query query = em.createQuery(findWithOutWhereClause);
        List<PersonRedis> results = query.getResultList();
        Assert.assertEquals(3, results.size());
    }

    /**
     * Find id by greater than and less than clause over non row key.
     */
    private void testFindByIdGTELTE()
    {
        Query query;
        List<PersonRedis> results;
        // Find id by greater than and less than clause over non row key
        String findIdByGTELTEClause = "Select p from PersonRedis p where p.age <=:max AND p.age>=:min";
        query = em.createQuery(findIdByGTELTEClause);
        query.setParameter("min", 32);
        query.setParameter("max", 35);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNotNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());
    }

    /**
     * Wait thread.
     * 
     * @param i
     *            the i
     */
    private void waitThread(int i)
    {

        try
        {
            Thread.sleep(i * 1000);

        }
        catch (InterruptedException e)
        {

            logger.info("Wait thread interrupted.");
        }

    }

    /**
     * Delete by query.
     */
    private void purge()
    {
        // Delete by query.
        String deleteQuery = "Delete from PersonRedis p";
        Query query = em.createQuery(deleteQuery);
        query.executeUpdate();
        em.clear();
        query = em.createQuery(deleteQuery);
        query.executeUpdate();
        em.flush();
        em.clear();
    }

    /**
     * Prepare person.
     * 
     * @param rowId
     *            the row id
     * @param age
     *            the age
     * @return the person redis
     */
    private PersonRedis preparePerson(String pname, String rowId, int age)
    {

        PersonRedis o = new PersonRedis();
        o.setPersonId(rowId);
        o.setPersonName(pname);
        o.setAge(age);
        o.setDay(Day.MONDAY);
        o.setMonth(Month.MARCH);
        return o;
    }

}