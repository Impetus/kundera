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
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.persistence.api.Batcher;

/**
 * Batch processing test case for cassandra.
 * 
 * @author vivek.mishra
 * 
 */
public class MongoBatchProcessorTest
{

    /**
     * persistence unit.
     */
    private static final String PERSISTENCE_UNIT = "MongoBatchTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    private List<PersonBatchMongoEntity> rows;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        em = emf.createEntityManager();
    }

    /**
     * Test case for batch operation.
     */
    @Test
    public void onBatch()
    {
        int counter = 0;
        rows = prepareData(10);
        for (PersonBatchMongoEntity entity : rows)
        {
            em.persist(entity);

            // check for implicit flush.
            if (++counter == 5)
            {
                Map<String, Client> clients = (Map<String, Client>) em.getDelegate();

                Batcher client = (Batcher) clients.get(PERSISTENCE_UNIT);
                Assert.assertEquals(5, client.getBatchSize());
                em.clear();
                for (int i = 0; i < 5; i++)
                {

                    // assert on each batch size record
                    Assert.assertNotNull(em.find(PersonBatchMongoEntity.class, rows.get(i).getPersonId()));

                    // as batch size is 5.
                    Assert.assertNull(em.find(PersonBatchMongoEntity.class, rows.get(6).getPersonId()));
                }
                // means implicit flush must happen
            }
        }

        // flush all on close.
        // explicit flush on close
        em.clear();
        em.flush();

        String sql = " Select p from PersonBatchMongoEntity p";
        Query query = em.createQuery(sql);
        List<PersonBatchMongoEntity> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(10, results.size());
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        for (PersonBatchMongoEntity o : rows)
        {
            em.remove(o);
        }

        MongoUtils.dropDatabase(emf, PERSISTENCE_UNIT);
        em.close();
        emf.close();
    }

    private List<PersonBatchMongoEntity> prepareData(Integer noOfRecords)
    {
        List<PersonBatchMongoEntity> persons = new ArrayList<PersonBatchMongoEntity>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            PersonBatchMongoEntity o = new PersonBatchMongoEntity();
            o.setPersonId(i + "");
            o.setPersonName("vivek" + i);
            o.setAge(10);
            persons.add(o);
        }

        return persons;
    }

}
