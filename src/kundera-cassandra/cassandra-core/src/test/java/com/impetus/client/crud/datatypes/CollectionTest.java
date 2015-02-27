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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.crud.datatypes.entities.EntityWithCollection;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.ObjectAccessor;
import com.impetus.kundera.property.accessor.UUIDAccessor;

/**
 * @author vivek.mishra
 * junit to test blob type over collection data type(e.g. Map,List and Set)  
 *
 */
public class CollectionTest
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
        Map<String, byte[]> byteCollection = new HashMap<String, byte[]>();
        
        ObjectAccessor accessor = new ObjectAccessor();
        accessor.toBytes(10);
        accessor.toBytes(100);
        
        byteCollection.put("key1", accessor.toBytes(10));
        byteCollection.put("key2", accessor.toBytes(11));
        EntityWithCollection entity = new EntityWithCollection();
        entity.setId("entityId1");
        entity.setDataMap(byteCollection);
        entity.setByteData(accessor.toBytes(100));
        em.persist(entity); // persist entity.
        
        em.clear();
        EntityWithCollection result = em.find(EntityWithCollection.class, "entityId1");
        Assert.assertNotNull(result);
        
        byte[] bytes = result.getDataMap().get("key1");
        byte[] newdata = result.getByteData();
        Object value = PropertyAccessorHelper.getObject(Object.class, bytes);

        Assert.assertEquals(10, value);
        
        // set list
        
        List<byte[]> listAsBytes= new ArrayList<byte[]>();
        
        UUID randomId = UUID.randomUUID();
        listAsBytes.add("Vivek".getBytes());
        listAsBytes.add(new UUIDAccessor().toBytes(randomId));
        
        result.setListAsBytes(listAsBytes);
        em.merge(result); // merge with list as bytes.
        
        em.clear();  // clear from cache.
        
        result = em.find(EntityWithCollection.class, "entityId1");
        Assert.assertNotNull(result);
        assertOnList(result, randomId);        
        
        Set<byte[]> setAsBytes = new HashSet<byte[]>();
        setAsBytes.add("Vivek".getBytes());
        setAsBytes.add(new UUIDAccessor().toBytes(randomId));
        result.setSetAsBytes(setAsBytes);
        em.merge(result); // merge with list as bytes.
        
        em.clear();
        result = em.find(EntityWithCollection.class, "entityId1");
        Assert.assertNotNull(result);
        assertOnList(result, randomId);
        
        Set<byte[]> resultAsBytes = result.getSetAsBytes();
        
     // on set type.
        for(byte[] recInBytes : resultAsBytes)
        {
            assertBytes(randomId, recInBytes);
        }
        // on set type.
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
    private void assertOnList(EntityWithCollection result, UUID randomId)
    {
        List<byte[]> lst = result.getListAsBytes();
        Assert.assertFalse(lst.isEmpty());
        Assert.assertEquals(2, lst.size());

        for (byte[] recInBytes : lst)
        {
            assertBytes(randomId, recInBytes);
        }
    }

    private void assertBytes(UUID randomId, byte[] recInBytes)
    {
        try
        {
            String str = (String) PropertyAccessorHelper.getObject(String.class, recInBytes);
            
            if(!str.equals("Vivek")) // string record is already compared
            {
                UUIDAccessor uuidAccessor = new UUIDAccessor();
                UUID recAsUUID = uuidAccessor.fromBytes(UUID.class, recInBytes);
                Assert.assertEquals(randomId, recAsUUID);
                
            } 
        }
        catch (Exception e)
        {
            Assert.fail();
        }
    }
    
    

}
