package com.impetus.client.crud;

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

public class PersonMongoTest extends BaseTest
{

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
        emf = Persistence.createEntityManagerFactory("mongoTest");
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    /**
     * On insert mongo.
     */
    @Test
    public void onInsertMongo()
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
    }

    /**
     * On merge mongo.
     */
    @Test
    public void onMergeMongo()
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
    }
}
