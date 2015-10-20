package com.impetus.client.generatedId;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.GenerationType;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.generatedId.entites.RedisGeneratedIdDefault;
import com.impetus.client.generatedId.entites.RedisGeneratedIdStrategyAuto;
import com.impetus.client.generatedId.entites.RedisGeneratedIdStrategyIdentity;
import com.impetus.client.generatedId.entites.RedisGeneratedIdStrategySequence;
import com.impetus.client.generatedId.entites.RedisGeneratedIdStrategyTable;
import com.impetus.client.generatedId.entites.RedisGeneratedIdWithOutSequenceGenerator;
import com.impetus.client.generatedId.entites.RedisGeneratedIdWithOutTableGenerator;
import com.impetus.client.generatedId.entites.RedisGeneratedIdWithSequenceGenerator;
import com.impetus.client.generatedId.entites.RedisGeneratedIdWithTableGenerator;
import com.impetus.client.redis.RedisClient;
import com.impetus.kundera.KunderaException;

public class RedisGeneratedIdTest
{

    private EntityManagerFactory emf;
    
    private EntityManager em;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    }

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("redis_pu");
        em = emf.createEntityManager();
    }

    @After
    public void tearDown() throws Exception
    { 

        purge();

        em.close();

        emf.close();
    }

    /**
     * 
     */
    private void purge()
    {
        // Delete by query.
        String deleteQuery = "Delete from RedisGeneratedIdStrategySequence r";
        Query query = em.createQuery(deleteQuery);
        query.executeUpdate();

        deleteQuery = "Delete from RedisGeneratedIdWithOutSequenceGenerator r";
        query = em.createQuery(deleteQuery);
        query.executeUpdate();

        deleteQuery = "Delete from RedisGeneratedIdWithSequenceGenerator r";
        query = em.createQuery(deleteQuery);
        query.executeUpdate();
    }

    @Test
    public void testPersist()
    {
        purge();
        RedisGeneratedIdDefault idDefault = new RedisGeneratedIdDefault();
        idDefault.setName("kuldeep");
        try
        {
            em.persist(idDefault);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.AUTO + " Strategy not supported by this client :" + RedisClient.class.getName(),
                    e.getMessage());
        }
        RedisGeneratedIdStrategyAuto strategyAuto = new RedisGeneratedIdStrategyAuto();
        strategyAuto.setName("kuldeep");
        em.clear();
        try
        {
            em.persist(strategyAuto);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.AUTO + " Strategy not supported by this client :" + RedisClient.class.getName(),
                    e.getMessage());
        }
        em.clear();
        RedisGeneratedIdStrategyIdentity strategyIdentity = new RedisGeneratedIdStrategyIdentity();
        strategyIdentity.setName("kuldeep");
        try
        {
            em.persist(strategyIdentity);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.UnsupportedOperationException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.IDENTITY + " Strategy not supported by this client :"
                            + RedisClient.class.getName(), e.getMessage());
        }
        em.clear();
        RedisGeneratedIdStrategySequence strategySequence = new RedisGeneratedIdStrategySequence();
        strategySequence.setName("Kuldeep");
        try
        {
            em.persist(strategySequence);
            List<RedisGeneratedIdStrategySequence> list = em.createQuery(
                    "Select c from RedisGeneratedIdStrategySequence c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kuldeep", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            strategySequence = em.find(RedisGeneratedIdStrategySequence.class, id);
            Assert.assertNotNull(strategySequence);
            Assert.assertEquals("Kuldeep", strategySequence.getName());
        }
        catch (KunderaException e)
        {
            
            Assert.fail();
        }
        em.clear();
        RedisGeneratedIdStrategyTable strategyTable = new RedisGeneratedIdStrategyTable();
        strategyTable.setName("KK");
        try
        {
            em.persist(strategyTable);
            Assert.fail();

        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.TABLE + " Strategy not supported by this client :" + RedisClient.class.getName(),
                    e.getMessage());
        }
        em.clear();
        RedisGeneratedIdWithOutSequenceGenerator withOutSequenceGenerator = new RedisGeneratedIdWithOutSequenceGenerator();
        withOutSequenceGenerator.setName("Kuldeep Kumar");
        try
        {
            em.persist(withOutSequenceGenerator);
            List<RedisGeneratedIdWithOutSequenceGenerator> list = em.createQuery(
                    "Select c from RedisGeneratedIdWithOutSequenceGenerator c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kuldeep Kumar", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            withOutSequenceGenerator = em.find(RedisGeneratedIdWithOutSequenceGenerator.class, id);
            Assert.assertNotNull(withOutSequenceGenerator);
            Assert.assertEquals("Kuldeep Kumar", withOutSequenceGenerator.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }
        em.clear();
        RedisGeneratedIdWithOutTableGenerator withOutTableGenerator = new RedisGeneratedIdWithOutTableGenerator();
        withOutTableGenerator.setName("Kuldeep Mishra");
        try
        {
            em.persist(withOutTableGenerator);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.TABLE + " Strategy not supported by this client :" + RedisClient.class.getName(),
                    e.getMessage());
        }
        em.clear();
        RedisGeneratedIdWithSequenceGenerator withSequenceGenerator = new RedisGeneratedIdWithSequenceGenerator();
        withSequenceGenerator.setName("Kuldeep Kumar Mishra");
        try
        {
            em.persist(withSequenceGenerator);
            List<RedisGeneratedIdWithSequenceGenerator> list = em.createQuery(
                    "Select c from RedisGeneratedIdWithSequenceGenerator c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kuldeep Kumar Mishra", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            withSequenceGenerator = em.find(RedisGeneratedIdWithSequenceGenerator.class, id);
            Assert.assertNotNull(withSequenceGenerator);
            Assert.assertEquals("Kuldeep Kumar Mishra", withSequenceGenerator.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }
        em.clear();
        RedisGeneratedIdWithTableGenerator withTableGenerator = new RedisGeneratedIdWithTableGenerator();
        withTableGenerator.setName("Kumar Mishra");
        try
        {
            em.persist(withTableGenerator);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.TABLE + " Strategy not supported by this client :" + RedisClient.class.getName(),
                    e.getMessage());
        }
    }

}
