/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.proxy.collection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FetchType;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.event.AddressEntity;
import com.impetus.kundera.polyglot.entities.AddressUMM;
import com.impetus.kundera.polyglot.entities.PersonUMMByMap;

/**
 * @author vivek.mishra
 * junit for {@link ProxyMap}
 */
public class ProxyMapTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    public void setup(final String persistenceUnit)
    {

        
        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
    }

    @Test
    public void test() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        setup("kunderatest");
        
        AddressEntity p = new AddressEntity();

        AddressEntity subaddress = new AddressEntity();

        Set<AddressEntity> subaddresses = new HashSet<AddressEntity>(1);
        subaddresses.add(subaddress);

        p.setSubaddresses(subaddresses);

        Relation relation = new Relation(AddressEntity.class.getDeclaredField("subaddresses"), AddressEntity.class,
                Set.class, FetchType.LAZY, null, false, null, ForeignKey.ONE_TO_MANY);
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        ProxyMap proxyMap = new ProxyMap(delegator, relation);

        proxyMap.setOwner(p);

        assertOnProxyMap(p, relation, delegator, proxyMap);
        
    }
    
    @Test
    public void testByMap() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        setup("patest");
        PersonUMMByMap person = new PersonUMMByMap();
        person.setPersonId("1");
        person.setPersonName("personName");
        
        AddressUMM address = new AddressUMM();
        
        Map<String,AddressUMM> addresses = new HashMap<String,AddressUMM>();
        addresses.put("addr1",address);
        
        person.setAddresses(addresses);

        Relation relation = new Relation(PersonUMMByMap.class.getDeclaredField("addresses"), AddressUMM.class,
                Set.class, FetchType.LAZY, null, false, null, ForeignKey.MANY_TO_MANY);
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        ProxyMap proxyMap = new ProxyMap(delegator, relation);

        proxyMap.setOwner(person);

        assertOnProxyMap(person, relation, delegator, proxyMap);
        
        java.util.List<PersonUMMByMap> personList = new ArrayList<PersonUMMByMap>();
        personList.add(person);
        
        Assert.assertEquals(1,proxyMap.size());
        Assert.assertEquals(1,proxyMap.values().size());
        
        Assert.assertNotNull(proxyMap.keySet());
        Assert.assertTrue(proxyMap.values().iterator().next() instanceof AddressUMM);
        
        Assert.assertTrue(proxyMap.containsKey("addr1"));
        Assert.assertTrue(proxyMap.containsValue(address));
        
        Assert.assertNotNull(proxyMap.get("addr1"));
        
        proxyMap.remove("addr1");
        
        Assert.assertNull(proxyMap.entrySet()); 
        
        proxyMap.clear();

        Assert.assertTrue(proxyMap.isEmpty());
        
        Assert.assertFalse(proxyMap.containsKey("addr1"));
        
        proxyMap.put("addr1", address);
        proxyMap.putAll(addresses);

        Assert.assertTrue(proxyMap.containsKey("addr1"));
        Assert.assertTrue(proxyMap.containsValue(address));

    }

    private void assertOnProxyMap(Object person, Relation relation, PersistenceDelegator delegator,
            ProxyMap proxyMap)
    {
        Assert.assertEquals(person, proxyMap.getOwner());
        Assert.assertNull(proxyMap.getDataCollection());
        Assert.assertNotNull(proxyMap.getRelation());
        Assert.assertEquals(relation, proxyMap.getRelation());
        Assert.assertEquals(delegator, proxyMap.getPersistenceDelegator());
        Assert.assertNotNull(proxyMap.getCopy());
        Assert.assertEquals(proxyMap.getRelation(),proxyMap.getCopy().getRelation());
    }
    

}
