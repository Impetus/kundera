/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.hbase.generatedId;

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

import com.impetus.client.hbase.HBaseClient;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdDefault;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdStrategyAuto;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdStrategyIdentity;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdStrategySequence;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdStrategyTable;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdWithOutSequenceGenerator;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdWithOutTableGenerator;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdWithSequenceGenerator;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdWithTableGenerator;
import com.impetus.client.hbase.generatedId.entites.HBaseGeneratedIdWithTableGeneratorWihtoutInit;
import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.KunderaException;


/**
 * @author Kuldeep Mishra
 *
 */
public class HBaseGeneratedIdTest
{
    private EntityManagerFactory emf;

    private HBaseCli cli;

    private static final String table = "kundera";

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
        cli = new HBaseCli();
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory("hbase_generated_id");
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
        cli.dropTable("kundera");

    }

    @Test
    public void testPersist()
    {
        EntityManager em = emf.createEntityManager();

        try
        {
            HBaseGeneratedIdDefault idDefault = new HBaseGeneratedIdDefault();
            idDefault.setName("kuldeep");
            em.persist(idDefault);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.AUTO + " Strategy not supported by this client :" + HBaseClient.class.getName(),
                    e.getMessage());
        }
        try
        {
            HBaseGeneratedIdStrategyAuto strategyAuto = new HBaseGeneratedIdStrategyAuto();
            strategyAuto.setName("kuldeep");
            em.persist(strategyAuto);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                    + GenerationType.AUTO + " Strategy not supported by this client :" + HBaseClient.class.getName(),
                    e.getMessage());
        }

        try
        {
            HBaseGeneratedIdStrategyIdentity strategyIdentity = new HBaseGeneratedIdStrategyIdentity();
            strategyIdentity.setName("kuldeep");
            em.persist(strategyIdentity);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.UnsupportedOperationException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.IDENTITY + " Strategy not supported by this client :"
                            + HBaseClient.class.getName(), e.getMessage());
        }

        HBaseGeneratedIdStrategySequence strategySequence = new HBaseGeneratedIdStrategySequence();
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
                            + HBaseClient.class.getName(), e.getMessage());
        }

        try
        {
            HBaseGeneratedIdStrategyTable strategyTable1 = new HBaseGeneratedIdStrategyTable();
            strategyTable1.setName("KK");
            em.persist(strategyTable1);

            HBaseGeneratedIdStrategyTable strategyTable2 = new HBaseGeneratedIdStrategyTable();
            strategyTable2.setName("vm");
            em.persist(strategyTable2);

            HBaseGeneratedIdStrategyTable strategyTable3 = new HBaseGeneratedIdStrategyTable();
            strategyTable3.setName("vs");
            em.persist(strategyTable3);

            HBaseGeneratedIdStrategyTable strategyTable = new HBaseGeneratedIdStrategyTable();
            strategyTable.setName("sh");
            em.persist(strategyTable);

            List<HBaseGeneratedIdStrategyTable> list = em.createQuery("Select c from HBaseGeneratedIdStrategyTable c")
                    .getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(4, list.size());
            for (HBaseGeneratedIdStrategyTable entity : list)
            {
                Assert.assertTrue(entity.getId() == 1 || entity.getId() == 51 || entity.getId() == 101
                        || entity.getId() == 151);
            }

            em.clear();
            strategyTable = em.find(HBaseGeneratedIdStrategyTable.class, strategyTable.getId());
            Assert.assertNotNull(strategyTable);
            Assert.assertEquals("sh", strategyTable.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }

        try
        {
            HBaseGeneratedIdWithOutSequenceGenerator withOutSequenceGenerator = new HBaseGeneratedIdWithOutSequenceGenerator();
            withOutSequenceGenerator.setName("Kuldeep Kumar");
            em.persist(withOutSequenceGenerator);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.SEQUENCE + " Strategy not supported by this client :"
                            + HBaseClient.class.getName(), e.getMessage());
        }

        try
        {
            HBaseGeneratedIdWithOutTableGenerator withOutTableGenerator = new HBaseGeneratedIdWithOutTableGenerator();
            withOutTableGenerator.setName("Kuldeep Mishra");
            em.persist(withOutTableGenerator);
            List<HBaseGeneratedIdWithOutTableGenerator> list = em.createQuery(
                    "Select c from HBaseGeneratedIdWithOutTableGenerator c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kuldeep Mishra", list.get(0).getName());
            Object id = list.get(0).getId();
            em.clear();
            withOutTableGenerator = em.find(HBaseGeneratedIdWithOutTableGenerator.class, id);
            Assert.assertNotNull(withOutTableGenerator);
            Assert.assertEquals("Kuldeep Mishra", withOutTableGenerator.getName());
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }
        try
        {
            HBaseGeneratedIdWithSequenceGenerator withSequenceGenerator = new HBaseGeneratedIdWithSequenceGenerator();
            withSequenceGenerator.setName("Kuldeep Kumar Mishra");
            em.persist(withSequenceGenerator);
            Assert.fail();
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: " + GenerationType.class.getSimpleName() + "."
                            + GenerationType.SEQUENCE + " Strategy not supported by this client :"
                            + HBaseClient.class.getName(), e.getMessage());
        }
        try
        {
          //Test with initValue paramater = 100 allocationsize = 30
            HBaseGeneratedIdWithTableGenerator withTableGenerator = new HBaseGeneratedIdWithTableGenerator();
            withTableGenerator.setName("Kumar Mishra");
            em.persist(withTableGenerator);
            List<HBaseGeneratedIdWithTableGenerator> list = em.createQuery(
                    "Select c from HBaseGeneratedIdWithTableGenerator c").getResultList();
            Assert.assertNotNull(list);
            Assert.assertEquals(1, list.size());
            Assert.assertEquals("Kumar Mishra", list.get(0).getName());
            Object id = list.get(0).getId();

            Assert.assertEquals(100, id);
            HBaseGeneratedIdWithTableGenerator withTableGenerator2 = new HBaseGeneratedIdWithTableGenerator();
            withTableGenerator2.setName("Kumar Mishra2");
            em.persist(withTableGenerator2);
            list = em.createQuery("Select c from HBaseGeneratedIdWithTableGenerator c").getResultList();
            Assert.assertEquals(2, list.size());
            
            if(list.get(0).getName().equals("Kumar Mishra2")){
            	  id = list.get(0).getId();
            }
            else{
                id = list.get(1).getId();
            }

            em.clear();
            withTableGenerator = em.find(HBaseGeneratedIdWithTableGenerator.class, id);
            Assert.assertNotNull(withTableGenerator);
            Assert.assertEquals("Kumar Mishra2", withTableGenerator.getName());
            
            //Test without initValue paramater allocationsize = 30
            HBaseGeneratedIdWithTableGeneratorWihtoutInit person1 = new HBaseGeneratedIdWithTableGeneratorWihtoutInit();
            person1.setName("pragalbh");
            em.persist(person1);

            HBaseGeneratedIdWithTableGeneratorWihtoutInit person2 = new HBaseGeneratedIdWithTableGeneratorWihtoutInit();
            person2.setName("pragalbh2");
            em.persist(person2);

            List<HBaseGeneratedIdWithTableGeneratorWihtoutInit> results = em.createQuery(
                    "Select c from HBaseGeneratedIdWithTableGeneratorWihtoutInit c").getResultList();
            Assert.assertTrue(results.get(0).getId() == 1 || results.get(0).getId() == 51);
            Assert.assertTrue(results.get(1).getId() == 1 || results.get(1).getId() == 51);
        }
        catch (KunderaException e)
        {
            Assert.fail();
        }
    }

}