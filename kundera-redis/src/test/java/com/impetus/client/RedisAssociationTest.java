package com.impetus.client;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.entities.AddressOTORedis;
import com.impetus.client.entities.PersonOTORedis;


public class RedisAssociationTest
{

    private static final String ROW_KEY = "1";

    /** The Constant REDIS_PU. */
    private static final String REDIS_PU = "redis_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(RedisAssociationTest.class);

    
    @Before
    public void setUp()
    {
//        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        emf = Persistence.createEntityManagerFactory(REDIS_PU);
    }
    
    @Test
    public void testCrud()
    {
        logger.info("On crud over association ");
        EntityManager em = emf.createEntityManager();
        
        PersonOTORedis person = new PersonOTORedis(ROW_KEY);
        person.setAge(32);
        person.setPersonName("vivek");
        AddressOTORedis address = new AddressOTORedis(12.23);
        address.setAddress("india");
        person.setAddress(address);
        em.persist(person);
        
        em.clear();
        PersonOTORedis p = em.find(PersonOTORedis.class, ROW_KEY);
        
        // Assertions.
        Assert.assertNotNull(p);
        Assert.assertEquals(person.getPersonId(), p.getPersonId());
        Assert.assertNotNull(p.getAddress());
        Assert.assertEquals(person.getAddress().getAddress(), p.getAddress().getAddress());

        // Remove
        em.remove(p);
        
        em.clear(); // clear cache
        Assert.assertNull(em.find(AddressOTORedis.class, 12.23));
    }
    
    
    @After
    public void tearDown()
    {
        logger.info("on tear down");
        emf.close();
    }
}
