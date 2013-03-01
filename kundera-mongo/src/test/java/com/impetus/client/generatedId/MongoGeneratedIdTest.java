package com.impetus.client.generatedId;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.GenerationType;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.generatedId.entites.MongoGeneratedIdDefault;
import com.impetus.client.generatedId.entites.MongoGeneratedIdStrategyAuto;
import com.impetus.client.generatedId.entites.MongoGeneratedIdStrategyIdentity;
import com.impetus.client.generatedId.entites.MongoGeneratedIdStrategySequence;
import com.impetus.client.generatedId.entites.MongoGeneratedIdStrategyTable;
import com.impetus.client.generatedId.entites.MongoGeneratedIdWithOutSequenceGenerator;
import com.impetus.client.generatedId.entites.MongoGeneratedIdWithOutTableGenerator;
import com.impetus.client.generatedId.entites.MongoGeneratedIdWithSequenceGenerator;
import com.impetus.client.generatedId.entites.MongoGeneratedIdWithTableGenerator;
import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.KunderaException;

public class MongoGeneratedIdTest
{

    EntityManagerFactory emf;

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
        emf = Persistence.createEntityManagerFactory("mongoTest");
    }

    @After
    public void tearDown() throws Exception
    {
    	MongoUtils.dropDatabase(emf, "mongoTest");
        emf.close();
    }

    @Test
    public void testPersist()
    {
        EntityManager em = emf.createEntityManager();

        MongoGeneratedIdDefault idDefault = new MongoGeneratedIdDefault();
        idDefault.setName("kuldeep");
        try
        {
            em.persist(idDefault);
            List<MongoGeneratedIdDefault> list = em.createQuery("Select c from MongoGeneratedIdDefault c")
                    .getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("kuldeep", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            idDefault = em.find(MongoGeneratedIdDefault.class, id);
            Assert.assertNotNull(idDefault);
            Assert.assertEquals("kuldeep", idDefault.getName());
        }
        catch (KunderaException e)
        {
        	e.printStackTrace();
            Assert.fail();
        }
        MongoGeneratedIdStrategyAuto strategyAuto = new MongoGeneratedIdStrategyAuto();
        strategyAuto.setName("kuldeep");
        try
        {
            em.persist(strategyAuto);
            List<MongoGeneratedIdStrategyAuto> list = em.createQuery("Select c from MongoGeneratedIdStrategyAuto c")
                    .getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("kuldeep", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            strategyAuto = em.find(MongoGeneratedIdStrategyAuto.class, id);
            Assert.assertNotNull(strategyAuto);
            Assert.assertEquals("kuldeep", strategyAuto.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }

        MongoGeneratedIdStrategyIdentity strategyIdentity = new MongoGeneratedIdStrategyIdentity();
        strategyIdentity.setName("kuldeep");
        try
        {
            em.persist(strategyIdentity);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.IDENTITY + " Strategy not supported by this client :"
                            + MongoDBClient.class.getName(), e.getMessage());
        }

        MongoGeneratedIdStrategySequence strategySequence = new MongoGeneratedIdStrategySequence();
        strategySequence.setName("Kuldeep");
        try
        {
            em.persist(strategySequence);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.SEQUENCE + " Strategy not supported by this client :"
                            + MongoDBClient.class.getName(), e.getMessage());
        }

        MongoGeneratedIdStrategyTable strategyTable = new MongoGeneratedIdStrategyTable();
        strategyTable.setName("KK");
        try
        {
            em.persist(strategyTable);
            Assert.fail();

        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.TABLE + " Strategy not supported by this client :"
                            + MongoDBClient.class.getName(), e.getMessage());
        }

        MongoGeneratedIdWithOutSequenceGenerator withOutSequenceGenerator = new MongoGeneratedIdWithOutSequenceGenerator();
        withOutSequenceGenerator.setName("Kuldeep Kumar");
        try
        {
            em.persist(withOutSequenceGenerator);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.SEQUENCE + " Strategy not supported by this client :"
                            + MongoDBClient.class.getName(), e.getMessage());
        }

        MongoGeneratedIdWithOutTableGenerator withOutTableGenerator = new MongoGeneratedIdWithOutTableGenerator();
        withOutTableGenerator.setName("Kuldeep Mishra");
        try
        {
            em.persist(withOutTableGenerator);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.TABLE + " Strategy not supported by this client :"
                            + MongoDBClient.class.getName(), e.getMessage());
        }
        MongoGeneratedIdWithSequenceGenerator withSequenceGenerator = new MongoGeneratedIdWithSequenceGenerator();
        withSequenceGenerator.setName("Kuldeep Kumar Mishra");
        try
        {
            em.persist(withSequenceGenerator);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.SEQUENCE + " Strategy not supported by this client :"
                            + MongoDBClient.class.getName(), e.getMessage());
        }
        MongoGeneratedIdWithTableGenerator withTableGenerator = new MongoGeneratedIdWithTableGenerator();
        withTableGenerator.setName("Kumar Mishra");
        try
        {
            em.persist(withTableGenerator);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.TABLE + " Strategy not supported by this client :"
                            + MongoDBClient.class.getName(), e.getMessage());
        }
    }
}
