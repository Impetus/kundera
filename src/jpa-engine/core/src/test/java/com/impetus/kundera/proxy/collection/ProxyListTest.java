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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FetchType;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.event.AddressEntityWithList;

/**
 * @author vivek.mishra
 *  junit for {@link ProxyList}
 */
public class ProxyListTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    @Before
    public void setup()
    {

        
        emf = Persistence.createEntityManagerFactory("kunderatest");
        em = emf.createEntityManager();

    }

    @Test
    public void test() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        AddressEntityWithList p = new AddressEntityWithList();
        p.setAddressId("addr1");
        p.setCity("noida");
        p.setStreet("street");
        AddressEntityWithList subaddress = new AddressEntityWithList();
        
        subaddress.setAddressId("subaddr1");
        p.setCity("noida");
        p.setStreet("sector 50");
        

        List<AddressEntityWithList> subaddresses = new ArrayList<AddressEntityWithList>(1);
        subaddresses.add(subaddress);

        p.setSubaddresses(subaddresses);

        Relation relation = new Relation(AddressEntityWithList.class.getDeclaredField("subaddresses"), AddressEntityWithList.class,
                List.class, FetchType.LAZY, null, false, null, ForeignKey.ONE_TO_MANY);
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        ProxyList proxyList = new ProxyList(delegator, relation);

        proxyList.setOwner(p);
        proxyList.add(0,p);
        
        
        
        Assert.assertTrue(proxyList.contains(p));
        Assert.assertEquals(p, proxyList.getOwner());
        Assert.assertNotNull(proxyList.getDataCollection());
        Assert.assertNotNull(proxyList.getRelation());
        Assert.assertEquals(relation, proxyList.getRelation());
        Assert.assertEquals(delegator, proxyList.getPersistenceDelegator());
        Assert.assertNotNull(proxyList.getCopy());
        Assert.assertEquals(proxyList.getRelation(),proxyList.getCopy().getRelation());

        proxyList.addAll(1,subaddresses);
        Assert.assertNotNull(proxyList.getDataCollection());
        Assert.assertEquals(2, ((Collection)proxyList.getDataCollection()).size());
        Assert.assertTrue(proxyList.contains(p));
        Assert.assertTrue(proxyList.containsAll(subaddresses));


        // TODO: This needs to fixed. There is an issue(runtime error) with proxy.remove(). Amresh need to fix it.
//        ProxyList.remove(p);
//        Assert.assertNotNull(ProxyList.getDataCollection());
//        Assert.assertEquals(1,ProxyList.getDataCollection().size());
//
//        ProxyList.removeAll(subaddresses);
//        Assert.assertNull(ProxyList.getDataCollection());

        proxyList.retainAll(subaddresses);
        Assert.assertNotNull(proxyList.getDataCollection());
        Assert.assertEquals(2, ((Collection)proxyList.getDataCollection()).size());
        
        Assert.assertNotNull(proxyList.get(0));
        Assert.assertNotNull(proxyList.get(1));
        Assert.assertSame(p,proxyList.get(0));
        Assert.assertSame(subaddress,proxyList.get(1));
        Assert.assertEquals(0,proxyList.indexOf(p));
        Assert.assertEquals(1,proxyList.lastIndexOf(subaddress));
        
        Assert.assertNotNull(proxyList.toArray());
        Assert.assertEquals(2,proxyList.size());
        Iterator<AddressEntityWithList> iter = proxyList.iterator();
        int counter = 0;
        while(iter.hasNext())
        {
            Assert.assertNotNull(iter.next());
            ++counter;
        }
        
        Assert.assertEquals(2, counter);
        
        Assert.assertNotNull(proxyList.subList(0, 1));
        Assert.assertEquals(2, proxyList.subList(0, 2).size());
        
       
        
        ListIterator<AddressEntityWithList> iterList = proxyList.listIterator();
        int counterList = 0;
        while(iterList.hasNext())
        {
            Assert.assertNotNull(iterList.next());
            ++counterList;
            
        }
        
        ListIterator<AddressEntityWithList> iterListWithArg = proxyList.listIterator(1);
        int counterListArg = 0;
        
        while(iterListWithArg.hasNext())
        {
        	
            Assert.assertNotNull(iterListWithArg.next());
            ++counterListArg;
            
        }
        
        proxyList.remove(1);
        Assert.assertEquals(2,proxyList.size());
        
        proxyList.set(1,p);
        
        proxyList.removeAll(new ArrayList());
        Assert.assertEquals(2,proxyList.size());
        
        proxyList.add("vivek1");
        Assert.assertEquals(3,proxyList.size());
        
        
        List lst = new ArrayList();
        lst.add("vivek");
        proxyList.addAll(lst);
        
        Assert.assertEquals(4,proxyList.size());
        
        proxyList.add("vivek1");
        
        Assert.assertEquals(4,proxyList.size());
        
        Assert.assertFalse(proxyList.isEmpty());
        
        proxyList.remove("vivek");
        Assert.assertEquals(3,proxyList.size());
        
      

        proxyList.clear();
        Assert.assertTrue(proxyList.isEmpty());

    }

}
