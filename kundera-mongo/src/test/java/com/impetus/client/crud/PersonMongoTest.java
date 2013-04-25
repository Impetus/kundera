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

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.PersonMongo.Day;
import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.client.Client;
import com.mongodb.CommandResult;

public class PersonMongoTest extends BaseTest
{

    private static final String _PU = "mongoTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

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
        
        findQuery = em.createQuery("Select p.age from PersonMongo p where p.personName = vivek");
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
        Assert.assertEquals("vivek", p.getPersonName());
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

        query = em.createNamedQuery("mongo.position.query");
        query.setParameter(1, "vivek");
        results = query.getResultList();
        Assert.assertEquals(3, results.size());

        query = em.createQuery("select p from PersonMongo p");
        query.setMaxResults(2);
        results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        
        selectIdQuery();
        
        onExecuteScript();

    }

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
        
        q = em.createQuery("Select p.personId from PersonMongo p where p.personName = vivek and p.age > "
                + 10);
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
        p.setPersonName("newvivek");
        em.merge(p);
        assertOnMerge(em, "PersonMongo", PersonMongo.class, "vivek", "newvivek", "personName");
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


    private void onExecuteScript()
    {
        Map<String, Client<Query>> clients = (Map<String, Client<Query>>) em.getDelegate();
        Client client = clients.get(_PU);

        String jScript = "db.system.js.save({ _id: \"echoFunction\",value : function(x) { return x; }})";
        Object result = ((MongoDBClient)client).executeScript(jScript);
        Assert.assertNull(result);
        String findOneJScript = "db.PERSON.findOne()";
        result = ((MongoDBClient)client).executeScript(findOneJScript);
        Assert.assertNotNull(result);
    }

}
