/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.crud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.persistence.api.Batcher;

/**
 * The Class HBaseBatchProcessorTest.
 * 
 * @author Devender Yadav
 */
public class HBaseBatchProcessorTest
{

    /** The Constant SCHEMA. */
    private static final String SCHEMA = "HBaseNew";

    /** The Constant HBASE_PU. */
    private static final String HBASE_PU = "crudTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The rows. */
    private List<PersonHBase> rows;

    /** The batch size. */
    private int BATCH_SIZE = 5;

    /** The property map. */
    private static Map<String, String> propertyMap = new HashMap<String, String>();

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        // as Batch size is 5.
        propertyMap.put("kundera.batch.size", "5");
        emf = Persistence.createEntityManagerFactory(HBASE_PU, propertyMap);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * Test case for batch operation.
     */
    @Test
    public void onBatch()
    {
        int counter = 1;
        rows = prepareData(10);
        for (PersonHBase entity : rows)
        {
            em.persist(entity);

            if (counter < BATCH_SIZE)
            {
                Assert.assertNull(em.find(PersonHBase.class, entity.getPersonId()));
            }
            else if (counter == BATCH_SIZE)
            {
                Map<String, Client> clients = (Map<String, Client>) em.getDelegate();

                Batcher client = (Batcher) clients.get(HBASE_PU);
                Assert.assertEquals(5, client.getBatchSize());
                em.clear();
                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    // assert on each batch size record
                    Assert.assertNotNull(em.find(PersonHBase.class, rows.get(i).getPersonId()));
                }
                counter = 0;
            }
            counter++;
        }

        em.clear();
        em.flush();

        String sql = " Select p from PersonHBase p";
        Query query = em.createQuery(sql);
        List<PersonHBase> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(10, results.size());

        for (PersonHBase entity : results)
        {
            entity.setPersonName("dev");
            em.merge(entity);
            if (counter < BATCH_SIZE)
            {
                PersonHBase p = em.find(PersonHBase.class, entity.getPersonId());
                Assert.assertEquals("vivek", p.getPersonName());
            }
            else if (counter == BATCH_SIZE)
            {
                em.clear();
                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    PersonHBase p = em.find(PersonHBase.class, results.get(i).getPersonId());
                    Assert.assertEquals("dev", p.getPersonName());
                }
                counter = 0;
            }
            counter++;
        }
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        emf.close();
        emf = null;
        HBaseTestingUtils.dropSchema(SCHEMA);
    }

    /**
     * Prepare data.
     * 
     * @param noOfRecords
     *            the no of records
     * @return the list
     */
    private List<PersonHBase> prepareData(Integer noOfRecords)
    {
        List<PersonHBase> persons = new ArrayList<PersonHBase>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            PersonHBase o = new PersonHBase();
            o.setPersonId(i + "");
            o.setPersonName("vivek");
            o.setAge(10);
            persons.add(o);
        }
        return persons;
    }

}