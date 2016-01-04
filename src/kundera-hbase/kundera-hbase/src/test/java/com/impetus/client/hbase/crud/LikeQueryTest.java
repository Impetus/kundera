package com.impetus.client.hbase.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.schemaManager.HBaseEntitySimple;

public class LikeQueryTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("hbase");
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    @Test
    public void likeQueryTest()
    {
        init();
        em.clear();

        String qry = "Select p from HBaseEntitySimple p where p.personName like :name";
        Query q = em.createQuery(qry);
        q.setParameter("name", "pragal");
        List<HBaseEntitySimple> persons = q.getResultList();
        assertNotNull(persons);
        Assert.assertEquals(1, persons.size());

        qry = "Select p from HBaseEntitySimple p where p.personName like :name";
        q = em.createQuery(qry);
        q.setParameter("name", "thik");
        persons = q.getResultList();
        assertEquals(1, persons.size());
    }

    private void init()
    {
        HBaseEntitySimple p1 = new HBaseEntitySimple();
        p1.setAge((short) 23);
        p1.setPersonId("1");
        p1.setPersonName("pragalbh garg");

        HBaseEntitySimple p2 = new HBaseEntitySimple();
        p2.setAge((short)20);
        p2.setPersonId("2");
        p2.setPersonName("karthik prasad");

        em.persist(p1);
        em.persist(p2);
    }

}