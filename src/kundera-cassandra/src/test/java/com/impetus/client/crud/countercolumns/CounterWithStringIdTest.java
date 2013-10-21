/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud.countercolumns;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.persistence.CassandraCli;

/**
 * @author vivek.mishra
 * Added junit for counter column test
 *
 */
public class CounterWithStringIdTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        Map<String, String> propertyMap = new HashMap<String, String>();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory("CassandraCounterTest", propertyMap);
        em = emf.createEntityManager();
    }

    @Test
    public void test()
    {
        CounterWithStringId entity = new CounterWithStringId();
        entity.setCounter(1);
        entity.setId("cnt1");
        em.persist(entity);
        
        em.clear();
        CounterWithStringId result = em.find(CounterWithStringId.class, "cnt1");
        
        Assert.assertNotNull(result);
        Assert.assertEquals("cnt1",result.getId());

        // increment counter
        result.setCounter(result.getCounter()+1);
        em.merge(result);
        
        em.clear();
        result = em.find(CounterWithStringId.class, "cnt1");
        
        Assert.assertNotNull(result);
        Assert.assertEquals("cnt1",result.getId());
        Assert.assertEquals(3,result.getCounter());


        
    }

    @After
    public void tearDown()
    {
        CassandraCli.dropKeySpace("KunderaCounterColumn");
        if (em != null)
        {
            em.close();
        }

        if (emf != null)
        {
            emf.close();
        }

    }
}
