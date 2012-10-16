/**
 * 
 */
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

import com.impetus.client.utils.MongoUtils;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MongoDBQueryOnIdTest extends BaseTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    private Map<Object, Object> col;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("mongoTest");
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        for (Object val : col.values())
        {
            em.remove(val);
        }
        MongoUtils.dropDatabase(emf, "mongoTest");
        em.close();
        emf.close();
    }

    @Test
    public void test()
    {
        init();
        em.clear();
        findById();
        findByWithOutWhereClause();
        findByIdEQ();
        findByIdLT();
        findByIdLTE();
        findByIdGT();
        findByIdGTE();
        findByIdGTEAndLT();
        findByIdGTAndLTE();
        findByIdGTAndAgeGTAndLT();
        findByIdGTEAndAge();
        findByIdLTEAndAge();
    }

    /**
     * 
     */
    private void findByWithOutWhereClause()
    {
        String qry = "Select p.personName from PersonMongo p";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonMongo person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);

    }

    /**
     * 
     */
    private void findByIdEQ()
    {
        String qry = "Select p.personName from PersonMongo p where p.personId = 2";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonMongo person : persons)
        {
            Assert.assertNull(person.getAge());
            Assert.assertEquals("2", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }

    }

    /**
     * 
     */
    private void findByIdLT()
    {
        String qry = "Select p.personName from PersonMongo p where p.personId < 3";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        int count = 0;
        for (PersonMongo person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
    }

    /**
     * 
     */
    private void findByIdLTE()
    {
        String qry = "Select p.personName, p.age from PersonMongo p where p.personId <= 3";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonMongo person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals(new Integer(20), person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertEquals(new Integer(15), person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertEquals(new Integer(10), person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    /**
     * 
     */
    private void findByIdGT()
    {
        String qry = "Select p.personName from PersonMongo p where p.personId > 1";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        int count = 0;
        for (PersonMongo person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("3", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
    }

    /**
     * 
     */
    private void findByIdGTE()
    {
        String qry = "Select p.personName from PersonMongo p where p.personId >= 1 ";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonMongo person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    /**
     * 
     */
    private void findByIdGTEAndLT()
    {
        String qry = "Select p.personName from PersonMongo p where p.personId >= 1 and p.personId < 3";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        int count = 0;
        for (PersonMongo person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("1", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(2, count);

    }

    /**
     * 
     */
    private void findByIdGTAndLTE()
    {
        String qry = "Select p.personName from PersonMongo p where p.personId > 1 and p.personId <= 2";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        int count = 0;
        for (PersonMongo person : persons)
        {
            Assert.assertNull(person.getAge());
            Assert.assertEquals("2", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
            count++;
        }
        Assert.assertEquals(1, count);
    }

    /**
     * 
     */
    private void findByIdGTAndAgeGTAndLT()
    {

        String qry = "Select p.personName from PersonMongo p where p.personId > 1 and p.age >=10 and p.age <= 20";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        int count = 0;
        for (PersonMongo person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertNull(person.getAge());
                Assert.assertEquals("3", person.getPersonId());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
    }

    /**
     * 
     */
    private void findById()
    {
        PersonMongo personHBase = findById(PersonMongo.class, "1", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(new Integer(10), personHBase.getAge());

        personHBase = findById(PersonMongo.class, "2", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(new Integer(20), personHBase.getAge());

        personHBase = findById(PersonMongo.class, "3", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(new Integer(15), personHBase.getAge());
    }

    /**
     * 
     */
    private void findByIdGTEAndAge()
    {
        String qry = "Select p.personName, p.age from PersonMongo p where p.personId >= 1 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonMongo person : persons)
        {
            Assert.assertEquals(new Integer(10), person.getAge());
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }
    }

    /**
     * 
     */
    private void findByIdLTEAndAge()
    {
        String qry = "Select p.personName, p.age from PersonMongo p where p.personId <= 3 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonMongo> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonMongo person : persons)
        {
            Assert.assertEquals(new Integer(10), person.getAge());
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }

    }

    private void init()
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
    }
}
