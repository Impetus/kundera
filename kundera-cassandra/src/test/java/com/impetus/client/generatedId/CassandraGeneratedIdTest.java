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

import com.impetus.client.cassandra.thrift.ThriftClient;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdDefault;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdStrategyAuto;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdStrategyIdentity;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdStrategySequence;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdStrategyTable;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdWithOutSequenceGenerator;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdWithOutTableGenerator;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdWithSequenceGenerator;
import com.impetus.client.generatedId.entites.CassandraGeneratedIdWithTableGenerator;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.KunderaException;

public class CassandraGeneratedIdTest
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
        CassandraCli.cassandraSetUp();
        emf = Persistence.createEntityManagerFactory("cassandra_generated_id");
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    @Test
    public void testPersist()
    {
        EntityManager em = emf.createEntityManager();

        CassandraGeneratedIdDefault idDefault = new CassandraGeneratedIdDefault();
        idDefault.setName("kuldeep");
        try
        {
            em.persist(idDefault);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.AUTO + " Strategy not supported by this client :" + ThriftClient.class.getName(),
                    e.getMessage());
        }
        CassandraGeneratedIdStrategyAuto strategyAuto = new CassandraGeneratedIdStrategyAuto();
        strategyAuto.setName("kuldeep");
        try
        {
            em.persist(strategyAuto);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.AUTO + " Strategy not supported by this client :" + ThriftClient.class.getName(),
                    e.getMessage());
        }

        CassandraGeneratedIdStrategyIdentity strategyIdentity = new CassandraGeneratedIdStrategyIdentity();
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
                            + ThriftClient.class.getName(), e.getMessage());
        }

        CassandraGeneratedIdStrategySequence strategySequence = new CassandraGeneratedIdStrategySequence();
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
                            + ThriftClient.class.getName(), e.getMessage());
        }

        CassandraGeneratedIdStrategyTable strategyTable = new CassandraGeneratedIdStrategyTable();
        strategyTable.setName("KK");
        try
        {
            em.persist(strategyTable);
            List<CassandraGeneratedIdStrategyTable> list = em.createQuery(
                    "Select c from CassandraGeneratedIdStrategyTable c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("KK", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            strategyTable = em.find(CassandraGeneratedIdStrategyTable.class, id);
            Assert.assertNotNull(strategyTable);
            Assert.assertEquals("KK", strategyTable.getName());

        }
        catch (KunderaException e)
        {
            Assert.fail();
        }

        CassandraGeneratedIdWithOutSequenceGenerator withOutSequenceGenerator = new CassandraGeneratedIdWithOutSequenceGenerator();
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
                            + ThriftClient.class.getName(), e.getMessage());
        }

        CassandraGeneratedIdWithOutTableGenerator withOutTableGenerator = new CassandraGeneratedIdWithOutTableGenerator();
        withOutTableGenerator.setName("Kuldeep Mishra");
        try
        {
            em.persist(withOutTableGenerator);
            List<CassandraGeneratedIdWithOutTableGenerator> list = em.createQuery(
                    "Select c from CassandraGeneratedIdWithOutTableGenerator c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kuldeep Mishra", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            withOutTableGenerator = em.find(CassandraGeneratedIdWithOutTableGenerator.class, id);
            Assert.assertNotNull(strategyTable);
            Assert.assertEquals("Kuldeep Mishra", withOutTableGenerator.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }
        CassandraGeneratedIdWithSequenceGenerator withSequenceGenerator = new CassandraGeneratedIdWithSequenceGenerator();
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
                            + ThriftClient.class.getName(), e.getMessage());
        }
        CassandraGeneratedIdWithTableGenerator withTableGenerator = new CassandraGeneratedIdWithTableGenerator();
        withTableGenerator.setName("Kumar Mishra");
        try
        {
            em.persist(withTableGenerator);
            List<CassandraGeneratedIdWithTableGenerator> list = em.createQuery(
                    "Select c from CassandraGeneratedIdWithTableGenerator c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kumar Mishra", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            withTableGenerator = em.find(CassandraGeneratedIdWithTableGenerator.class, id);
            Assert.assertNotNull(strategyTable);
            Assert.assertEquals("Kumar Mishra", withTableGenerator.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }
    }
}
