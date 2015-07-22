package com.impetus.client.generatedId;

/**
 * Copyright 2015 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.GenerationType;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
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
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

public class CustomGeneratedIdTest
{

    private EntityManagerFactory emf;

    protected Map<String, String> properties = new HashMap<String, String>();

    @Before
    public void setUp() throws Exception
    {
        properties.put(PersistenceProperties.KUNDERA_AUTO_GENERATOR_CLASS, "com.impetus.client.generatedId.CustomIdGenerator");
        CassandraCli.cassandraSetUp();
        if (properties.isEmpty())
        {
            emf = Persistence.createEntityManagerFactory("cassandra_generated_id");
        }
        else
        {
            emf = Persistence.createEntityManagerFactory("cassandra_generated_id", properties);
        }
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

        try
        {
            CassandraGeneratedIdDefault idDefault = new CassandraGeneratedIdDefault();
            idDefault.setName("kuldeep");
            em.persist(idDefault);
            List<CassandraGeneratedIdDefault> list = em.createQuery("Select c from CassandraGeneratedIdDefault c")
                    .getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("kuldeep", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            
            idDefault = em.find(CassandraGeneratedIdDefault.class, id);
            Assert.assertNotNull(idDefault);
            Assert.assertEquals("kuldeep", idDefault.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }

        try
        {
            CassandraGeneratedIdStrategyAuto strategyAuto = new CassandraGeneratedIdStrategyAuto();
            strategyAuto.setName("kuldeep");
            em.persist(strategyAuto);
            List<CassandraGeneratedIdStrategyAuto> list = em.createQuery(
                    "Select c from CassandraGeneratedIdStrategyAuto c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("kuldeep", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            strategyAuto = em.find(CassandraGeneratedIdStrategyAuto.class, id);
            Assert.assertNotNull(strategyAuto);
            Assert.assertEquals("kuldeep", strategyAuto.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }

        try
        {
            CassandraGeneratedIdStrategyIdentity strategyIdentity = new CassandraGeneratedIdStrategyIdentity();
            strategyIdentity.setName("kuldeep");
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

        try
        {
            CassandraGeneratedIdStrategySequence strategySequence = new CassandraGeneratedIdStrategySequence();
            strategySequence.setName("Kuldeep");
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

        try
        {
            CassandraGeneratedIdStrategyTable strategyTable = new CassandraGeneratedIdStrategyTable();
            strategyTable.setName("KK");
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

        try
        {
            CassandraGeneratedIdWithOutSequenceGenerator withOutSequenceGenerator = new CassandraGeneratedIdWithOutSequenceGenerator();
            withOutSequenceGenerator.setName("Kuldeep Kumar");
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

        try
        {
            CassandraGeneratedIdWithOutTableGenerator withOutTableGenerator = new CassandraGeneratedIdWithOutTableGenerator();
            withOutTableGenerator.setName("Kuldeep Mishra");
            em.persist(withOutTableGenerator);
            List<CassandraGeneratedIdWithOutTableGenerator> list = em.createQuery(
                    "Select c from CassandraGeneratedIdWithOutTableGenerator c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kuldeep Mishra", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            withOutTableGenerator = em.find(CassandraGeneratedIdWithOutTableGenerator.class, id);
            Assert.assertNotNull(withOutTableGenerator);
            Assert.assertEquals("Kuldeep Mishra", withOutTableGenerator.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }
        try
        {
            CassandraGeneratedIdWithSequenceGenerator withSequenceGenerator = new CassandraGeneratedIdWithSequenceGenerator();
            withSequenceGenerator.setName("Kuldeep Kumar Mishra");
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
        try
        {
            CassandraGeneratedIdWithTableGenerator withTableGenerator = new CassandraGeneratedIdWithTableGenerator();
            withTableGenerator.setName("Kumar Mishra");
            em.persist(withTableGenerator);
            List<CassandraGeneratedIdWithTableGenerator> list = em.createQuery(
                    "Select c from CassandraGeneratedIdWithTableGenerator c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kumar Mishra", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            withTableGenerator = em.find(CassandraGeneratedIdWithTableGenerator.class, id);
            Assert.assertNotNull(withTableGenerator);
            Assert.assertEquals("Kumar Mishra", withTableGenerator.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }
    }

}
