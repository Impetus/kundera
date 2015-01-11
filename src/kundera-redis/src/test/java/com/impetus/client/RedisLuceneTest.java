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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.entities.Month;
import com.impetus.client.entities.PersonRedis;
import com.impetus.client.entities.PersonRedis.Day;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * @author amitkumar
 * 
 */
public class RedisLuceneTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redis_pu";

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisESIndexerTest.class);

    protected Map<String, String> propertyMap = new HashMap<String, String>();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        propertyMap.put("index.home.dir", "./lucene");
        emf = Persistence.createEntityManagerFactory("redisLucene_pu", propertyMap);
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {

        LuceneCleanupUtilities.cleanDir("./lucene");
        LuceneCleanupUtilities.cleanLuceneDirectory(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getPersistenceUnitMetadata("redisLucene_pu"));
        // Delete by query.
        String deleteQuery = "Delete from PersonRedis p";
        Query query = em.createQuery(deleteQuery);
        int updateCount = query.executeUpdate();
        em.close();
        emf.close();
    }

    @Test
    public void crudTestWithLucene()
    {

        logger.info("Crud tests for ES");

        PersonRedis person1 = preparePerson("101", 20);
        PersonRedis person2 = preparePerson("102", 40);
        PersonRedis person3 = preparePerson("103", 60);

        // Persist records
        em.persist(person1);
        em.persist(person2);
        em.persist(person3);

        PersonRedis fetchPerson = em.find(PersonRedis.class, "102");
        // Assertion for fetching objects
        Assert.assertEquals("102", fetchPerson.getPersonId());
        Assert.assertEquals("Amit", fetchPerson.getPersonName());
        Assert.assertEquals(40, fetchPerson.getAge().intValue());

        fetchPerson.setAge(50);
        em.merge(fetchPerson);

        fetchPerson = em.find(PersonRedis.class, "102");
        // Assertion for merge
        Assert.assertEquals("102", fetchPerson.getPersonId());
        Assert.assertEquals("Amit", fetchPerson.getPersonName());
        Assert.assertEquals(50, fetchPerson.getAge().intValue());

        em.remove(fetchPerson);
        // Assertion for remove
        fetchPerson = null;
        fetchPerson = em.find(PersonRedis.class, "102");
        Assert.assertNull(fetchPerson);

        em.remove(em.find(PersonRedis.class, "101"));
        em.remove(em.find(PersonRedis.class, "103"));
    }

    @Test
    public void queryTestWithLucene()
    {
        init();
        em.clear();

        String qry = "Select p.personName, p.age from PersonRedis p where p.personId = 1 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonRedis> persons = q.getResultList();
        assertNotNull(persons);
        assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertEquals(10, persons.get(0).getAge().intValue());
        assertEquals("Amit", persons.get(0).getPersonName());

        qry = "Select p.personName from PersonRedis p where p.age=20";
        q = em.createQuery(qry);
        persons = q.getResultList();
        assertNotNull(persons);
        assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertNull(persons.get(0).getAge());
        assertEquals("Amit", persons.get(0).getPersonName());

        qry = "Select p.age from PersonRedis p where p.personId = 2";
        q = em.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertNull(persons.get(0).getPersonName());
        assertEquals(20, persons.get(0).getAge().intValue());

        qry = "Select p from PersonRedis p";
        q = em.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
    }

    private void init()
    {
        Object p1 = preparePerson("1", 10);
        Object p2 = preparePerson("2", 20);
        Object p3 = preparePerson("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
    }

    private PersonRedis preparePerson(String rowId, int age)
    {

        PersonRedis o = new PersonRedis();
        o.setPersonId(rowId);
        o.setPersonName("Amit");
        o.setAge(age);
        o.setDay(Day.MONDAY);
        o.setMonth(Month.MARCH);
        return o;
    }
}
