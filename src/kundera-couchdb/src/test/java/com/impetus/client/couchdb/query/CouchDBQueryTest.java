/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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

package com.impetus.client.couchdb.query;

import java.util.List;

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

import com.impetus.client.couchdb.datatypes.tests.CouchDBBase;
import com.impetus.client.couchdb.entities.PersonCouchDB;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.query.QueryHandlerException;

/**
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBQueryTest extends CouchDBBase
{

    private static final String ROW_KEY = "1";

    /** The emf. */
    private EntityManagerFactory emf;

    private EntityManager em;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CouchDBQueryTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(pu);
        em = emf.createEntityManager();
        super.setUpBase(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
        init();
    }

    @Test
    public void testPopulateEntites()
    {
        logger.info("On testPopulateEntities");

        final String originalName = "vivek";

        // Find without where clause.
        String findWithOutWhereClause = "Select p from PersonCouchDB p";
        Query query = em.createQuery(findWithOutWhereClause);
        List<PersonCouchDB> results = query.getResultList();
        Assert.assertEquals(3, results.size());

        // find by key.
        String findById = "Select p from PersonCouchDB p where p.personId=:personId";
        query = em.createQuery(findById);
        query.setParameter("personId", ROW_KEY);
        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());

        // Find by key and now row key
        String findByAge = "Select p from PersonCouchDB p where p.age=:age";
        query = em.createQuery(findByAge);
        query.setParameter("age", 32);

        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());
        Assert.assertEquals(ROW_KEY, results.get(0).getPersonId());

        // Find by key and now row key
        String findByIdAndAge = "Select p from PersonCouchDB p where p.personId=:personId AND p.age=:age";
        query = em.createQuery(findByIdAndAge);
        query.setParameter("personId", ROW_KEY);
        query.setParameter("age", 32);

        results = query.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());

        // find by between over non rowkey
        String findAgeByBetween = "Select p from PersonCouchDB p where p.age between :min AND :max";
        query = em.createQuery(findAgeByBetween);
        query.setParameter("min", 32);
        query.setParameter("max", 35);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());

        // Between clause over rowkey
        String findIdByBetween = "Select p from PersonCouchDB p where p.personId between :min AND :max";
        query = em.createQuery(findIdByBetween);
        query.setParameter("min", ROW_KEY);
        query.setParameter("max", ROW_KEY + 1);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());

        // Find by greater than and less than clause over non row key
        String findAgeByGTELTEClause = "Select p from PersonCouchDB p where p.age <=:max AND p.age>=:min";
        query = em.createQuery(findAgeByGTELTEClause);
        query.setParameter("min", 32);
        query.setParameter("max", 35);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(originalName, results.get(0).getPersonName());

        // Find id by greater than and less than clause over non row key
        String findIdByGTELTEClause = "Select p from PersonCouchDB p where p.age <=:max AND p.age>=:min";
        query = em.createQuery(findIdByGTELTEClause);
        query.setParameter("min", 32);
        query.setParameter("max", 35);

        results = query.getResultList();
        Assert.assertEquals(2, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNotNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());

        // Invalid scenario.
        try
        {
            // String invalidDifferentClause =
            // "Select p from PersonCouchDB p where p.personId=:personId AND p.age >=:age";
            // query = em.createQuery(invalidDifferentClause);
            // query.setParameter("personId", ROW_KEY);
            // query.setParameter("age", 32);
            // query.getResultList();
            // Assert.fail("Must have thrown query handler exception!");
        }

        catch (QueryHandlerException qhex)
        {
            Assert.assertNotNull(qhex);
        }

        // Delete by query.
        String deleteQuery = "Delete from PersonCouchDB p";
        query = em.createQuery(deleteQuery);
        int updateCount = query.executeUpdate();

        Assert.assertEquals(3, updateCount);

        // Search all after delete.
        findWithOutWhereClause = "Select p from PersonCouchDB p";
        query = em.createQuery(findWithOutWhereClause);
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertTrue(results.isEmpty());
    }

    @Test
    public void testSelectedFields()
    {
        /*
         * Test for selecting specific fields IMPORTANT NOTE: Selecting specific
         * fields with 'WHERE' clause is yet not supported. It works only
         * without 'WHERE' clause.
         */

        logger.info("On testSelectedFields");

        Query query = em.createQuery("Select p.age from PersonCouchDB p");
        List results = query.getResultList();
        Assert.assertEquals(3, results.size());
        int age = (int) results.get(0);
        Assert.assertTrue(age == 29 || age == 32 || age == 34);
        age = (int) results.get(1);
        Assert.assertTrue(age == 29 || age == 32 || age == 34);
        age = (int) results.get(2);
        Assert.assertTrue(age == 29 || age == 32 || age == 34);

        query = em.createQuery("Select p.age, p.personName from PersonCouchDB p");
        results = query.getResultList();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(2, ((List) results.get(0)).size());
        age = (int) ((List) results.get(0)).get(0);
        Assert.assertTrue(age == 29 || age == 32 || age == 34);
        Assert.assertEquals("vivek", (String) ((List) results.get(0)).get(1));
    }

    @Test
    public void testAggregations()
    {
        /*
         * Test for selecting specific fields IMPORTANT NOTE: Selecting specific
         * fields with 'WHERE' clause is yet not supported. It works only
         * without 'WHERE' clause.
         */

        logger.info("On testAggregations");

        Query query = em.createQuery("Select count(p) from PersonCouchDB p");
        List results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, (int) results.get(0));

        query = em.createQuery("Select count(p.age) from PersonCouchDB p");
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, (int) results.get(0));

        query = em.createQuery("Select sum(p.age) from PersonCouchDB p");
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(95.0, (double) results.get(0));

        query = em.createQuery("Select min(p.age) from PersonCouchDB p");
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(29.0, (double) results.get(0));

        query = em.createQuery("Select max(p.age) from PersonCouchDB p");
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(34.0, (double) results.get(0));

        query = em.createQuery("Select avg(p.age) from PersonCouchDB p");
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(31.666666666666668, (double) results.get(0));
    }

    private void init()
    {
        String name = "vivek";
        persistObject(name, 32, ROW_KEY);
        persistObject(name, 34, ROW_KEY + 1);
        persistObject(name, 29, ROW_KEY + 3);
    }

    private void persistObject(String name, int age, String id)
    {
        PersonCouchDB object = new PersonCouchDB();
        object.setAge(age);
        object.setPersonId(id);
        object.setPersonName(name);
        em.persist(object);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        emf = null;
        super.dropDatabase();
    }

}
