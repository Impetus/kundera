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
package com.impetus.kundera.client.cassandra.crud;

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
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.PersonCassandra;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.persistence.api.Batcher;

/**
 * Batch processing test case for cassandra.
 * 
 * @author vivek.mishra
 * 
 */
public class CassandraBatchProcessorTest
{

    /**
     * 
     */
    private static final String PERSISTENCE_UNIT = "cassandra_ds_pu";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    protected Map propertyMap = null;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

        // cassandraSetUp();
        CassandraCli.cassandraSetUp();
        // CassandraCli.initClient();
        CassandraCli.createKeySpace("KunderaExamples");

        if (propertyMap == null)
        {
            propertyMap = new HashMap();
            propertyMap.put("kundera.batch.size", "5");
        }
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT, propertyMap);
        em = emf.createEntityManager();
    }

    @Test
    public void onBatch()
    {
        int counter = 0;
        List<PersonCassandra> rows = prepareData(10);
        for (PersonCassandra entity : rows)
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
                    Assert.assertNotNull(em.find(PersonCassandra.class, rows.get(i).getPersonId()));

                    // as batch size is 5.
                    Assert.assertNull(em.find(PersonCassandra.class, rows.get(6).getPersonId()));
                }
                // means implicit flush must happen
            }
        }

        // flush all on close.
        // explicit flush on close
        em.clear();
        em.close();

        em = emf.createEntityManager();

        String sql = " Select p from PersonCassandra p";
        Query query = em.createQuery(sql);
        List<PersonCassandra> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(10, results.size());
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    	em.close();
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }


    private List<PersonCassandra> prepareData(Integer noOfRecords)
    {
        List<PersonCassandra> persons = new ArrayList<PersonCassandra>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            PersonCassandra o = new PersonCassandra();
            o.setPersonId(i + "");
            o.setPersonName("vivek" + i);
            o.setAge(10);
            persons.add(o);
        }

        return persons;
    }

}
