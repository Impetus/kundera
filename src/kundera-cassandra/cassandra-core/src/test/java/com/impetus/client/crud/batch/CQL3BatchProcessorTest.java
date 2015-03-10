/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.cassandra.thrift.cql.CQLUser;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.persistence.api.Batcher;

/**
 * @author Chhavi Gangwal
 * 
 */
public class CQL3BatchProcessorTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    private String persistenceUnit = "cassandra_cql";

    /**
     * setup the PU
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        Map propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create-drop");

        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        propertyMap.put(PersistenceProperties.KUNDERA_BATCH_SIZE, "5");

        emf = Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
        em = emf.createEntityManager();
    }

    /**
     * closes em
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        emf = null;
    }

    /**
     * tests batch execution process for cql3
     */
    @Test
    public void testBatcherForCQL3()
    {

        Map<String, Integer> ttlValues = new HashMap<String, Integer>();
        ttlValues.put("CQLUSER", new Integer(5));
        em.setProperty("ttl.per.request", true);
        em.setProperty("ttl.values", ttlValues);
        em.setProperty(PersistenceProperties.KUNDERA_BATCH_SIZE, 5);
        int counter = 0;
        List<CQLUser> rows = prepareData(10);
        for (CQLUser entity : rows)
        {
            em.persist(entity);

            // check for implicit flush.
            if (++counter == 5)
            {
                Map<String, Client> clients = (Map<String, Client>) em.getDelegate();

                Batcher client = (Batcher) clients.get(persistenceUnit);
                Assert.assertEquals(5, client.getBatchSize());
                // em.clear();
                for (int i = 0; i < 5; i++)
                {

                    // assert on each batch size record
                    Assert.assertNotNull(em.find(CQLUser.class, rows.get(i).getId()));

                    Assert.assertNull(em.find(CQLUser.class, rows.get(9).getId()));
                }
                // means implicit flush must happen
            }
        }

        // flush all on close.
        // explicit flush on close
        em.clear();
        
        
        String sql = " Delete from CQLUser p";
        Query query = em.createQuery(sql);
        List<PersonBatchCassandraEntity> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(0, results.size());
        em.close();
    }

    /**
     * prepare teh dat afor batch execution
     */
    private List<CQLUser> prepareData(Integer noOfRecords)
    {
        List<CQLUser> persons = new ArrayList<CQLUser>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            CQLUser user = new CQLUser();
            user.setId(i);
            user.setName("Kuldeep");
            user.setAge(24);
            persons.add(user);
        }

        return persons;
    }

}
