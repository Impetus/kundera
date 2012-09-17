/**
 * 
 */
package com.impetus.client.hbase.crud;

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

import com.impetus.client.hbase.junits.HBaseCli;

/**
 * @author Kuldeep Mishra
 * 
 */
public class HBaseIdQueryTest extends BaseTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    private Map<Object, Object> col;

    private HBaseCli cli;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        cli = new HBaseCli();
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory("hbaseTest");
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
        em.close();
        emf.close();
        if (cli != null && cli.isStarted())
        {
            cli.stopCluster("PERSON");
        }
        LuceneCleanupUtilities.cleanLuceneDirectory("hbaseTest");
    }

    @Test
    public void test()
    {
        init();
        em.clear();
        findById();
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
    private void findByIdEQ()
    {
        String qry = "Select p.personName from PersonHBase p where p.personId = 2";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonHBase person : persons)
        {
            Assert.assertEquals("2", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }

    }

    /**
     * 
     */
    private void findByIdLT()
    {
        String qry = "Select p.personName from PersonHBase p where p.personId < 3";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        int count = 0;
        for (PersonHBase person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
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
        String qry = "Select p.personName, p.age from PersonHBase p where p.personId <= 3";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        int count = 0;
        for (PersonHBase person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals(20, person.getAge());
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
                Assert.assertEquals(10, person.getAge());
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
    private void findByIdGT()
    {
        String qry = "Select p.personName from PersonHBase p where p.personId > 1";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonHBase person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
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
    private void findByIdGTE()
    {
        String qry = "Select p.personName from PersonHBase p where p.personId >= 1 ";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonHBase person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
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
        String qry = "Select p.personName from PersonHBase p where p.personId >= 1 and p.personId < 3";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        int count = 0;
        for (PersonHBase person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
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
        String qry = "Select p.personName from PersonHBase p where p.personId > 1 and p.personId <= 2";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        int count = 0;
        for (PersonHBase person : persons)
        {
            Assert.assertEquals("1", person.getPersonId());
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

        String qry = "Select p.personName from PersonHBase p where p.personId > 1 and p.age >=10 and p.age <= 20";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
        int count = 0;
        for (PersonHBase person : persons)
        {
            if (person.getPersonId().equals("2"))
            {
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else if (person.getPersonId().equals("3"))
            {
                Assert.assertEquals("vivek", person.getPersonName());
                count++;
            }
            else
            {
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
    private void findById()
    {
        PersonHBase personHBase = findById(PersonHBase.class, "1", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(10, personHBase.getAge());

        personHBase = findById(PersonHBase.class, "2", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(20, personHBase.getAge());

        personHBase = findById(PersonHBase.class, "3", em);
        Assert.assertNotNull(personHBase);
        Assert.assertEquals("vivek", personHBase.getPersonName());
        Assert.assertEquals(15, personHBase.getAge());
    }

    /**
     * 
     */
    private void findByIdGTEAndAge()
    {
        String qry = "Select p.personName, p.age from PersonHBase p where p.personId >= 1 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonHBase person : persons)
        {
            Assert.assertEquals(10, person.getAge());
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }
    }

    /**
     * 
     */
    private void findByIdLTEAndAge()
    {
        String qry = "Select p.personName, p.age from PersonHBase p where p.personId <= 3 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        for (PersonHBase person : persons)
        {
            Assert.assertEquals(10, person.getAge());
            Assert.assertEquals("1", person.getPersonId());
            Assert.assertEquals("vivek", person.getPersonName());
        }

    }

    private void init()
    {

        cli.createTable("PERSON");
        cli.addColumnFamily("PERSON", "PERSON_NAME");
        cli.addColumnFamily("PERSON", "AGE");
        Object p1 = prepareHbaseInstance("1", 10);
        Object p2 = prepareHbaseInstance("2", 20);
        Object p3 = prepareHbaseInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
    }
}
