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
package com.impetus.client.crud.datatypes;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * @author vivek.mishra
 * junit for unicode character test. 
 */
public class EntityWithUnicodeTest
{

    private EntityManagerFactory emf;
    private EntityManager em;

    
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        Map<String, String> propertyMap = new HashMap<String, String>();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory("cassandra_pu", propertyMap);
        em = emf.createEntityManager();

    }

    @Test
    public void test()
    {
        String uniCode = "中文";
        UUID id = UUID.randomUUID();
        EntityWithUnicode entity = new EntityWithUnicode();
        entity.setDesc(uniCode);
        entity.setId(id);
        em.persist(entity);

        em.clear(); // clear persistence cache.
        EntityWithUnicode result = em.find(EntityWithUnicode.class, id);
        Assert.assertEquals(uniCode,result.getDesc());
        
    }

    @After
    public void tearDown()
    {
        CassandraCli.dropKeySpace("KunderaTests");
        if(em != null)
        {
            em.close();
        }
        
        if(emf != null)
        {
            emf.close();
        }
        
        
    }

    
    
    
}
