/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.crud;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.Day;
import com.impetus.client.crud.entities.PersonBatchMongoEntity;
import com.impetus.client.crud.entities.PersonMongo;
import com.impetus.client.crud.entities.PersonMongo.Month;
import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.client.Client;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

/**
 * The Class PersonMongoTest.
 */
public class PersonMongoTest extends BaseTest
{

    /** The Constant _PU. */
    private static final String _PU = "mongoTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /** The col. */
    private Map<Object, Object> col;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(_PU);
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    /**
     * On insert mongo.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onInsertMongo() throws Exception
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);

        Query findQuery = em.createQuery("Select p from PersonMongo p");
        List<PersonMongo> allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = em.createQuery("Select p from PersonMongo p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        em.clear();
        PersonMongo p = findById(PersonMongo.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals(Day.FRIDAY, p.getDay());
        Assert.assertEquals(Month.JAN, p.getMonth());
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertNotNull(p.getMap());
        Assert.assertFalse(p.getMap().isEmpty());
        Assert.assertEquals(2, p.getMap().size());

        assertFindByName(em, "PersonMongo", PersonMongo.class, "vivek", "personName");
        assertFindByNameAndAge(em, "PersonMongo", PersonMongo.class, "vivek", "10", "personName");
        assertFindByNameAndAgeGTAndLT(em, "PersonMongo", PersonMongo.class, "vivek", "10", "20", "personName");
        assertFindByNameAndAgeBetween(em, "PersonMongo", PersonMongo.class, "vivek", "10", "15", "personName");
        assertFindByRange(em, "PersonMongo", PersonMongo.class, "1", "2", "personId");
        assertFindWithoutWhereClause(em, "PersonMongo", PersonMongo.class);

        Query query = em.createNamedQuery("mongo.named.query");
        query.setParameter("name", "vivek");
        List<PersonMongo> results = query.getResultList();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(Month.JAN, results.get(0).getMonth());

        query = em.createNamedQuery("mongo.position.query");
        query.setParameter(1, "vivek");
        results = query.getResultList();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(Month.JAN, results.get(0).getMonth());

        findQuery = em.createQuery("Select p from PersonMongo p where p.day = :day");
        findQuery.setParameter("day", Day.FRIDAY);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(3, allPersons.size());

        query = em.createQuery("select p from PersonMongo p");
        query.setMaxResults(2);
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(Month.JAN, results.get(0).getMonth());

        query = em.createQuery("select p from PersonMongo p");
        query.setMaxResults(1);
        PersonMongo result = (PersonMongo) (query.getSingleResult());
        Assert.assertNotNull(result);
        Assert.assertEquals(Month.JAN, result.getMonth());

        query = em.createQuery("select p from PersonMongo p where p.personName = kuldeep");
        try
        {
            result = (PersonMongo) (query.getSingleResult());
            Assert.fail("Should have gone to catch block!");
        }
        catch (NoResultException nrex)
        {
            Assert.assertNotNull(nrex.getMessage());
        }

        selectIdQuery();
        onExecuteScript();
        onExecuteNativeQuery();

    }

    /**
     * Select id query.
     */
    private void selectIdQuery()
    {
        String query = "select p.personId from PersonMongo p";
        Query q = em.createQuery(query);
        List<PersonMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        query = "Select p.personId from PersonMongo p where p.personName = vivek";
        // // find by name.
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        query = "Select p.personId from PersonMongo p where p.personName = vivek";
        // // find by name.
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());

        q = em.createQuery("Select p.personId from PersonMongo p where p.personName = vivek and p.age > " + 10);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(2, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNull(results.get(0).getAge());
    }

    /**
     * On typed named query.
     */
    @Test
    public void onNamedTypedQuery()
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        TypedQuery<PersonMongo> query = em.createNamedQuery("mongo.named.query", PersonMongo.class);
        query.setParameter("name", "vivek");
        List<PersonMongo> results = query.getResultList();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(Month.JAN, results.get(0).getMonth());
    }

    /**
     * On generic typed named query.
     */
    @Test
    public void onGenericTypedNamedQuery()
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        TypedQuery<Object> query = em.createNamedQuery("mongo.named.query", Object.class);
        query.setParameter("name", "vivek");
        List<Object> results = query.getResultList();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(PersonMongo.class, results.get(0).getClass());
    }

    /**
     * On invalid typed query.
     * 
     */
    @Test
    public void onInvalidTypedNamedQuery()
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        TypedQuery<PersonBatchMongoEntity> query = null;
        try
        {
            query = em.createNamedQuery("mongo.named.query", PersonBatchMongoEntity.class);
            query.setParameter("name", "vivek");
            Assert.fail("Should have gone to catch block, as it is an invalid scenario!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNull(query);
        }
    }

    /**
     * On merge mongo.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onMergeMongo() throws Exception
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        PersonMongo p = findById(PersonMongo.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        // modify record.
        p.setPersonName("Mc.John Doe");
        em.merge(p);
        assertOnMerge(em, "PersonMongo", PersonMongo.class, "vivek", "Mc.John Doe", "personName");
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {/*
      * Delete is working, but as row keys are not deleted from cassandra, so
      * resulting in issue while reading back. // Delete
      * em.remove(em.find(Person.class, "1")); em.remove(em.find(Person.class,
      * "2")); em.remove(em.find(Person.class, "3")); em.close(); emf.close();
      * em = null; emf = null;
      */
        for (Object val : col.values())
        {
            em.remove(val);
        }
        MongoUtils.dropDatabase(emf, _PU);
        emf.close();

    }

    /**
     * On execute script.
     */
    private void onExecuteScript()
    {

        Map<String, Client<Query>> clients = (Map<String, Client<Query>>) em.getDelegate();
        Client client = clients.get(_PU);

        /*
         * To find a single document from DB
         */
        String script = "db.PERSON.findOne()";
        Object result = (client).executeScript(script);
        Assert.assertNotNull(result);

        /*
         * Aggregation in mongoDB match query on PERSON_NAME is done and JSON
         * list of type BasicDBList is returned this JSON object can be easily
         * converted to java PersonMongo Object using GSON, etc.
         */
        script = "db.PERSON.aggregate([{ $match: { \"PERSON_NAME\": \"vivek\" } } ]).toArray()";
        result = (client).executeScript(script);
        BasicDBList resultList = (BasicDBList) result;
        Assert.assertEquals(3, resultList.size());
        Assert.assertNotNull(result);

        /*
         * To count number of documents in an collection
         */

        script = "db.PERSON.count()";
        result = (client).executeScript(script);
        long totalDocuments = ((Double) result).longValue();
        Assert.assertEquals(3, totalDocuments);

        /*
         * 
         * To get an array of distinct values
         */

        script = "db.PERSON.distinct(\"PERSON_NAME\")";
        result = (client).executeScript(script);
        resultList = (BasicDBList) result;
        Assert.assertEquals("vivek", resultList.get(0));
        Assert.assertEquals(1, resultList.size());

        /*
         * 
         * Native Query on the Key of Map<String, Month> map
         */
        script = "db.PERSON.find({'map.first month':'JAN'}).toArray()";
        result = (client).executeScript(script);
        resultList = (BasicDBList) result;
        Assert.assertEquals(3, resultList.size());

        /*
         * 
         * Native Query to find a person with age > 15
         */
        script = "db.PERSON.find({ \"AGE\": { $gt: 15 } }).toArray()";
        result = (client).executeScript(script);
        resultList = (BasicDBList) result;
        Assert.assertEquals(1, resultList.size());

    }

    /**
     * On execute native query.
     */
    private void onExecuteNativeQuery()
    {

        /**
         * 
         * In case of Native Find Queries, Criteria can be written directly
         * 
         */

        String test = "{\"AGE\":10}";
        List<PersonMongo> list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(new Integer(10), list.get(0).getAge());

        test = "{ \"PERSON_NAME\":\"vivek\"}";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(3, list.size());
        Assert.assertEquals("vivek", list.get(0).getPersonName());
        Assert.assertEquals("vivek", list.get(1).getPersonName());
        Assert.assertEquals("vivek", list.get(2).getPersonName());

        /**
         * 
         * Matches values that are greater than the value specified in the
         * query.
         * 
         */
        test = "{ \"AGE\": { $gt: 12 } }";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(2, list.size());
        for (PersonMongo person : list)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals(new Integer(20), person.getAge());
            }
            else
            {
                Assert.assertEquals(new Integer(15), person.getAge());
            }
        }

        /**
         * 
         * Matches values that are greater than or equal to the value specified
         * in the query.
         * 
         */
        test = "{ \"AGE\": { $gte: 15 } }";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(2, list.size());
        for (PersonMongo person : list)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals(new Integer(20), person.getAge());
            }
            else
            {
                Assert.assertEquals(new Integer(15), person.getAge());
            }
        }

        /**
         * 
         * Matches values that are less than the value specified in the query.
         * 
         */
        test = "{ \"AGE\": { $lt: 15 } }";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(new Integer(10), list.get(0).getAge());

        /**
         * 
         * Matches values that are less than or equal to the value specified in
         * the query.
         * 
         */
        test = "{ \"AGE\": { $lte: 15 } }";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(2, list.size());
        for (PersonMongo person : list)
        {
            if (person.getPersonId().equals("1"))
            {
                Assert.assertEquals(new Integer(10), person.getAge());
            }
            else
            {
                Assert.assertEquals(new Integer(15), person.getAge());
            }
        }

        /**
         * 
         * Matches all values that are not equal to the value specified in the
         * query.
         * 
         */
        test = "{ \"AGE\": { $ne: 10 } }";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(2, list.size());
        for (PersonMongo person : list)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals(new Integer(20), person.getAge());
            }
            else
            {
                Assert.assertEquals(new Integer(15), person.getAge());
            }
        }

        /**
         * 
         * Matches any of the values that exist in an array specified in the
         * query.
         * 
         */
        test = "{ \"AGE\": { $in: [10,15] } }";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(2, list.size());
        for (PersonMongo person : list)
        {
            if (person.getPersonId().equals("1"))
            {
                Assert.assertEquals(new Integer(10), person.getAge());
            }
            else
            {
                Assert.assertEquals(new Integer(15), person.getAge());
            }
        }

        /**
         * 
         * Joins query clauses with a logical AND returns all documents that
         * match the conditions of both clauses.
         * 
         */
        test = "{ $and : [{\"_id\":\"1\"},{ \"AGE\": 10 }]}";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(new Integer(10), list.get(0).getAge());
        Assert.assertEquals("1", list.get(0).getPersonId());

        /**
         * 
         * Joins query clauses with a logical OR returns all documents that
         * match the conditions of either clause.
         * 
         */
        test = "{ $or : [{\"_id\":\"1\"},{ \"AGE\": 20 }]}";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(2, list.size());
        for (PersonMongo person : list)
        {
            if (person.getPersonId().equals("1"))
            {
                Assert.assertEquals(new Integer(10), person.getAge());
            }
            else
            {
                Assert.assertEquals(new Integer(20), person.getAge());
            }
        }

        test = "db.PERSON.findOne()";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(1, list.size());

        test = "db.PERSON.find({ \"PERSON_NAME\":\"vivek\"})";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(3, list.size());
        Assert.assertEquals("vivek", list.get(0).getPersonName());
        Assert.assertEquals("vivek", list.get(1).getPersonName());
        Assert.assertEquals("vivek", list.get(2).getPersonName());

        test = "db.PERSON.aggregate([{ $match: { \"PERSON_NAME\": \"vivek\" } } ])";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals(3, list.size());
        Assert.assertEquals("vivek", list.get(0).getPersonName());
        Assert.assertEquals("vivek", list.get(1).getPersonName());
        Assert.assertEquals("vivek", list.get(2).getPersonName());

        test = "db.PERSON.count()";
        List count = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals((long) 3, count.get(0));

        test = "db.PERSON.dataSize()";
        count = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertNotNull(count);

        /**
         * total allocated size to a collection
         */
        test = "db.PERSON.storageSize()";
        count = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertNotNull(count);

        test = "db.PERSON.totalIndexSize()";
        count = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertNotNull(count);

        /**
         * Total size of Collection (documents+indexes)
         * 
         */
        test = "db.PERSON.totalSize()";
        count = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertNotNull(count);

        test = "db.PERSON.distinct(\"PERSON_NAME\")";
        List distinctList = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals("vivek", distinctList.get(0));

        test = "db.PERSON.findAndModify({query: { \"AGE\": 10},update: { $set: { \"PERSON_NAME\": \"dev\" } },new:true,upsert:true })";
        list = em.createNativeQuery(test, PersonMongo.class).getResultList();
        Assert.assertEquals("dev", list.get(0).getPersonName());
    }

    /**
     * Increment function test.
     */
    @Test
    public void incrementFunctionTest()
    {
        Object p1 = prepareMongoInstance("1", 10);
        em.persist(p1);

        String updateFunc = "UPDATE PersonMongo p set p.age = INCREMENT(1) where p.personId = :personId";
        Query q = em.createQuery(updateFunc);
        q.setParameter("personId", "1");
        Assert.assertEquals(1, q.executeUpdate());

        updateFunc = "UPDATE PersonMongo set p.age = DECREMENT(1) where p.personId = :personId";
        q = em.createQuery(updateFunc);
        q.setParameter("personId", "1");
        Assert.assertEquals(1, q.executeUpdate());

        updateFunc = "UPDATE PersonMongo p set p.age = DECREMENT(2)";
        q = em.createQuery(updateFunc);
        Assert.assertEquals(1, q.executeUpdate());

        em.clear();

        String query = "Select p from PersonMongo p ";
        q = em.createQuery(query);
        List<PersonMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(new Integer(8), results.get(0).getAge());

        updateFunc = "UPDATE PersonMongo p set p.age = INCREMENT(5)";
        q = em.createQuery(updateFunc);
        Assert.assertEquals(1, q.executeUpdate());

        em.clear();

        query = "Select p from PersonMongo p ";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(new Integer(13), results.get(0).getAge());
    }

    /**
     * Sub query test.
     */
    @Test
    public void subQueryTest()
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        String query = "Select p from PersonMongo p where p.personName <> :name and p.age NOT IN :ageList"
                + " and (p.personId = :personId)";
        Query q = em.createQuery(query);
        q.setParameter("name", "vivek");
        q.setParameter("ageList", new ArrayList<Integer>()
        {
            {
                add(20);
                add(21);
            }
        });
        q.setParameter("personId", "1");
        List<PersonMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());

        query = "Select p from PersonMongo p where (p.personName = :name and p.age NOT IN :ageList)"
                + " and (p.personId = :personId)";
        q = em.createQuery(query);
        q.setParameter("name", "vivek");
        q.setParameter("ageList", new ArrayList<Integer>()
        {
            {
                add(20);
                add(21);
            }
        });
        q.setParameter("personId", "1");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNotNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());

    }

    /**
     * Inter clause operator test.
     */
    @Test
    public void interClauseOperatorTest()
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        String query = "Select p from PersonMongo p where (p.personName = :name OR p.age NOT IN :ageList)"
                + " AND (p.personId = :personId)";
        Query q = em.createQuery(query);
        q.setParameter("name", "vivek");
        q.setParameter("ageList", new ArrayList<Integer>()
        {
            {
                add(10);
                add(20);
            }
        });
        q.setParameter("personId", "1");
        List<PersonMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("1", results.get(0).getPersonId());

        query = "Select p from PersonMongo p where (p.personName = :name AND p.age NOT IN :ageList)"
                + " OR (p.personId = :personId)";
        q = em.createQuery(query);
        q.setParameter("name", "vivek");
        q.setParameter("ageList", new ArrayList<Integer>()
        {
            {
                add(10);
                add(21);
            }
        });
        q.setParameter("personId", "1");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

        query = "Select p from PersonMongo p where (p.personName = :name OR p.age NOT IN :ageList)"
                + " OR (p.personId = :personId) ORDER BY p.age";
        q = em.createQuery(query);
        q.setParameter("name", "vivek");
        q.setParameter("ageList", new ArrayList<Integer>()
        {
            {
                add(10);
                add(21);
            }
        });
        q.setParameter("personId", "1");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNotNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());

        Assert.assertEquals("1", results.get(0).getPersonId());
        Assert.assertEquals("2", results.get(2).getPersonId());

    }

    /**
     * Pagination query test.
     */
    @Test
    public void paginationQueryTest()
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        String query = "Select p from PersonMongo p ";
        Query q = em.createQuery(query);
        q.setFirstResult(1);
        q.setMaxResults(3);
        List<PersonMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        query = "Select p from PersonMongo p where (p.personName = :name and p.age NOT IN :ageList)"
                + " and (p.personId = :personId)";
        q = em.createQuery(query);
        q.setFirstResult(0);
        q.setMaxResults(3);
        q.setParameter("name", "vivek");
        q.setParameter("ageList", new ArrayList<Integer>()
        {
            {
                add(20);
                add(21);
            }
        });
        q.setParameter("personId", "1");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertNotNull(results.get(0).getPersonId());
        Assert.assertNotNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());

    }

    /**
     * Map reduce test (simple).
     */
    @Test
    public void mapReduceTest()
    {
        String query = "db.PERSON.mapReduce(\n" + "  function () { emit( this.AGE - this.AGE % 10, 1 ); },\n"
                + "  function (key, values) { return { age: key, count: Array.sum(values) }; },\n"
                + "  { query: {}, out: { inline: 1 } }\n" + ")";

        executeMapReduceTest(query);
    }

    /**
     * Map reduce test (with db.getCollection(..)).
     */
    @Test
    public void mapReduceTestWithGetCollection()
    {
        String query = "db.getCollection('PERSON').mapReduce(\n"
                + "  function () { emit( this.AGE - this.AGE % 10, 1 ); },\n"
                + "  function (key, values) { return { age: key, count: Array.sum(values) }; },\n"
                + "  { query: {}, out: { inline: 1 } }\n" + ")";

        executeMapReduceTest(query);

        query = "db.getCollection(\"PERSON\").mapReduce(\n"
                + "  function () { emit( this.AGE - this.AGE % 10, 1 ); },\n"
                + "  function (key, values) { return { age: key, count: Array.sum(values) }; },\n"
                + "  { query: {}, out: { inline: 1 } }\n" + ")";

        executeMapReduceTest(query);
    }

    @Test
    public void testUpperAndLower()
    {
        Object p1 = prepareMongoInstance("alexander", 10);
        Object p2 = prepareMongoInstance("sandra", 20);
        Object p3 = prepareMongoInstance("CASSANDRA", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        String query = "Select p from PersonMongo p where UPPER(p.personId) = 'SANDRA'";
        Query q = em.createQuery(query);
        List<PersonMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        query = "Select p from PersonMongo p where LOWER(p.personId) = 'cassandra'";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        query = "Select p from PersonMongo p where LOWER(p.personId) IN ('sandra', 'cassandra')";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        query = "Select p from PersonMongo p where LOWER(p.personId) NOT IN ('sandra', 'cassandra')";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        query = "Select p from PersonMongo p where UPPER(p.personId) <> 'ALEXANDER'";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testLikeAndUpperLower()
    {
        Object p1 = prepareMongoInstance("alexander", 10);
        Object p2 = prepareMongoInstance("sandra", 20);
        Object p3 = prepareMongoInstance("CASSANDRA", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        String query = "Select p from PersonMongo p where p.personId LIKE '%and%'";
        Query q = em.createQuery(query);
        List<PersonMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        query = "Select p from PersonMongo p where UPPER(p.personId) LIKE '%and%'";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

        query = "Select p from PersonMongo p where LOWER(p.personId) LIKE '%and%'";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
    }

    @Test
    public void testNotLike()
    {
        Object p1 = prepareMongoInstance("alexander", 10);
        Object p2 = prepareMongoInstance("sandra", 20);
        Object p3 = prepareMongoInstance("CASSANDRA", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        String query = "Select p from PersonMongo p where p.personId NOT LIKE '%andr%'";
        Query q = em.createQuery(query);
        List<PersonMongo> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        query = "Select p from PersonMongo p where p.personId NOT LIKE '%andr%' AND p.personId = 'alexander'";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        query = "Select p from PersonMongo p where p.personId = 'alexander' AND p.personId NOT LIKE '%andr%'";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        query = "Select p from PersonMongo p where p.personId LIKE '%lex%' AND p.personId NOT LIKE 'cass%'";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
    }

    /**
     * Execute map reduce test.
     * 
     * @param query
     *            the query
     */
    private void executeMapReduceTest(String query)
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        Object p4 = prepareMongoInstance("4", 12);
        Object p5 = prepareMongoInstance("5", 19);
        Object p6 = prepareMongoInstance("6", 23);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        em.persist(p4);
        em.persist(p5);
        em.persist(p6);

        Query q = em.createNativeQuery(query);
        List<BasicDBObject> results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        for (BasicDBObject item : results)
        {
            Assert.assertTrue(item.containsField("value"));

            BasicDBObject value = (BasicDBObject) item.get("value");

            Assert.assertTrue(value.containsField("age"));
            Assert.assertTrue(value.containsField("count"));

            int age = value.getInt("age");
            int count = value.getInt("count");

            if (age == 10)
            {
                Assert.assertEquals(4, count);
            }
            else if (age == 20)
            {
                Assert.assertEquals(2, count);
            }
            else
            {
                Assert.fail("Unexpected result");
            }
        }
    }

    /**
     * Count test.
     */
    @Test
    public void countTest()
    {
        Object p1 = prepareMongoInstance("1", 10);
        Object p2 = prepareMongoInstance("2", 20);
        Object p3 = prepareMongoInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        Query query = em.createQuery("select count(p) from PersonMongo p");
        List<?> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), 1);
        Assert.assertEquals(results.get(0), 3L);

        query = em.createQuery("select count(p) from PersonMongo p where p.age < 18");
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), 1);
        Assert.assertEquals(results.get(0), 2L);

        query = em.createQuery("select count(p) from PersonMongo p");
        Object singleResult = query.getSingleResult();
        Assert.assertEquals(singleResult, 3L);
    }

}
