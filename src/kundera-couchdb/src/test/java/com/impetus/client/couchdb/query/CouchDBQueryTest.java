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

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(CouchDBQueryTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(pu);
        super.setUpBase();
    }

    @Test
    public void testPopulateEntites()
    {
        logger.info("On testPopulateEntities");

        EntityManager em = emf.createEntityManager();

        final String originalName = "vivek";

        // persist record.
        PersonCouchDB object = new PersonCouchDB();
        object.setAge(32);
        object.setPersonId(ROW_KEY);
        object.setPersonName(originalName);

        em.persist(object);

        object.setAge(34);
        object.setPersonId(ROW_KEY + 1);
        object.setPersonName(originalName);

        em.persist(object);

        object.setAge(29);
        object.setPersonId(ROW_KEY + 3);
        object.setPersonName(originalName);

        em.persist(object);

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
