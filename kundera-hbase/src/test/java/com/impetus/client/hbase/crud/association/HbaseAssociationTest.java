package com.impetus.client.hbase.crud.association;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.client.twitter.entities.PersonalDetailHbase;

public class HbaseAssociationTest
{

    private static final String ROW_KEY = "1";

    /** The Constant REDIS_PU. */
    private static final String HBASE_PU = "hbaseTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(HbaseAssociationTest.class);

    @Before
    public void setUp()
    {
        HBaseCli  cli = new HBaseCli();
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
    }

    @Test
    public void testCrud()
    {
        EntityManager em = emf.createEntityManager();

        PersonOTOHbase person = new PersonOTOHbase(ROW_KEY);
        person.setAge(32);
        person.setPersonName("vivek");
        AddressOTOHbase address = new AddressOTOHbase(12.23);
        address.setAddress("india");
        person.setAddress(address);

        PersonalDetailHbase detail = new PersonalDetailHbase();
        detail.setName("KK");
        detail.setPassword("xxxxxxx");
        detail.setPersonalDetailId("xyz");
        detail.setRelationshipStatus("single");

        person.setPersonalDetail(detail);

        em.persist(person);

        em.clear();
        PersonOTOHbase p = em.find(PersonOTOHbase.class, ROW_KEY);

        // Assertions.
        Assert.assertNotNull(p);
        Assert.assertEquals(person.getPersonId(), p.getPersonId());
        Assert.assertNotNull(p.getAddress());
        Assert.assertEquals(person.getAddress().getAddress(), p.getAddress().getAddress());
        Assert.assertNotNull(p.getPersonalDetail());
        Assert.assertNotNull(p.getPersonalDetail().getName());
        Assert.assertNotNull(p.getPersonalDetail().getPassword());
        Assert.assertNotNull(p.getPersonalDetail().getPersonalDetailId());
        Assert.assertNotNull(p.getPersonalDetail().getRelationshipStatus());

        // Remove
        em.remove(p);

        em.clear(); // clear cache
        Assert.assertNull(em.find(AddressOTOHbase.class, 12.23));
    }

    @After
    public void tearDown()
    {

    }
}
