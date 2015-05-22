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
package com.impetus.client.crud;

import java.util.ArrayList;
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

import com.impetus.client.crud.entities.PersonBatchMongoEntity;
import com.impetus.client.mongodb.MongoDBClientProperties;
import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.persistence.api.Batcher;
import com.mongodb.WriteConcern;

/**
 * Batch processing test case for MongoDB.
 * 
 * @author Devender Yadav
 * 
 */
public class MongoBatchProcessorTest
{

    /**
     * persistence unit.
     */
    private static final String MONGO_PU = "MongoBatchTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /** The rows. */
    private List<PersonBatchMongoEntity> rows;

    /** The batch size. */
    private int BATCH_SIZE = 5;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(MONGO_PU);
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

        /*
         * By default Bulk Operation is UNORDERED. But you can set this to
         * ORDERED/UNORDERED using boolean true/false for a particular em
         * instance
         */
        em.setProperty(MongoDBClientProperties.ORDERED_BULK_OPERATION, true);

        /*
         * 
         * By default WriteConcern is ACKNOWLEDGED. But it can be changed to any
         * desired value for a particular em instance
         */
        em.setProperty(MongoDBClientProperties.WRITE_CONCERN, new WriteConcern(1));
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
        for (PersonBatchMongoEntity o : rows)
        {
            em.remove(o);
        }
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
        MongoUtils.dropDatabase(emf, MONGO_PU);
    }

    /**
     * Test case for batch operation.
     */
    @Test
    public void onBatch()
    {
        int counter = 1;
        rows = prepareData(5);
        for (PersonBatchMongoEntity entity : rows)
        {
            em.persist(entity);
            if (counter < BATCH_SIZE)
            {
                Assert.assertNull(em.find(PersonBatchMongoEntity.class, entity.getPersonId()));
            }

            else if (counter == BATCH_SIZE)
            {
                Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
                Batcher client = (Batcher) clients.get(MONGO_PU);
                Assert.assertEquals(5, client.getBatchSize());
                em.clear();
                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    Assert.assertNotNull(em.find(PersonBatchMongoEntity.class, rows.get(i).getPersonId()));
                }
                counter = 0;
            }
            counter++;
        }
        em.clear();

        String sql = " Select p from PersonBatchMongoEntity p";
        Query query = em.createQuery(sql);
        List<PersonBatchMongoEntity> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(5, results.size());

        for (PersonBatchMongoEntity entity : results)
        {
            entity.setPersonName("dev");
            em.merge(entity);
            if (counter < BATCH_SIZE)
            {
                PersonBatchMongoEntity p = em.find(PersonBatchMongoEntity.class, entity.getPersonId());
                Assert.assertEquals("vivek", p.getPersonName());
            }
            else if (counter == BATCH_SIZE)
            {
                em.clear();
                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    PersonBatchMongoEntity p = em.find(PersonBatchMongoEntity.class, results.get(i).getPersonId());
                    Assert.assertEquals("dev", p.getPersonName());
                }
                counter = 0;
            }
            counter++;
        }
    }

    /**
     * Prepare data.
     * 
     * @param noOfRecords
     *            the no of records
     * @return the list
     */
    private List<PersonBatchMongoEntity> prepareData(Integer noOfRecords)
    {
        List<PersonBatchMongoEntity> persons = new ArrayList<PersonBatchMongoEntity>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            PersonBatchMongoEntity o = new PersonBatchMongoEntity();
            o.setPersonId(i + "");
            o.setPersonName("vivek");
            o.setAge(20);
            persons.add(o);
        }

        return persons;
    }

}