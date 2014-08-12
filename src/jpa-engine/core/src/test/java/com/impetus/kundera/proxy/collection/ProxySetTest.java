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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FetchType;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.event.AddressEntity;

/**
 * @author vivek.mishra junit for {@link ProxySet}
 */
public class ProxySetTest
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
        AddressEntity p = new AddressEntity();

        AddressEntity subaddress = new AddressEntity();

        Set<AddressEntity> subaddresses = new HashSet<AddressEntity>(1);
        subaddresses.add(subaddress);

        p.setSubaddresses(subaddresses);

        Relation relation = new Relation(AddressEntity.class.getDeclaredField("subaddresses"), AddressEntity.class,
                Set.class, FetchType.LAZY, null, false, null, ForeignKey.ONE_TO_MANY);
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        ProxySet proxySet = new ProxySet(delegator, relation);

        proxySet.setOwner(p);
        proxySet.add(p);

        Assert.assertTrue(proxySet.contains(p));
        Assert.assertEquals(p, proxySet.getOwner());
        Assert.assertNotNull(proxySet.getDataCollection());
        Assert.assertNotNull(proxySet.getRelation());
        Assert.assertEquals(relation, proxySet.getRelation());
        Assert.assertEquals(delegator, proxySet.getPersistenceDelegator());
        Assert.assertNotNull(proxySet.getCopy());
        Assert.assertEquals(proxySet.getRelation(), proxySet.getCopy().getRelation());

        proxySet.addAll(subaddresses);
        Assert.assertNotNull(proxySet.getDataCollection());
        Assert.assertEquals(2, ((Collection) proxySet.getDataCollection()).size());
        Assert.assertTrue(proxySet.contains(p));
        Assert.assertTrue(proxySet.containsAll(subaddresses));

        // TODO: This needs to fixed. There is an issue(runtime error) with
        // proxy.remove(). Amresh need to fix it.
        // proxySet.remove(p);
        // Assert.assertNotNull(proxySet.getDataCollection());
        // Assert.assertEquals(1,proxySet.getDataCollection().size());
        //
        // proxySet.removeAll(subaddresses);
        // Assert.assertNull(proxySet.getDataCollection());

        proxySet.retainAll(subaddresses);
        Assert.assertNotNull(proxySet.getDataCollection());
        Assert.assertEquals(2, ((Collection) proxySet.getDataCollection()).size());

        Iterator<AddressEntity> iter = proxySet.iterator();
        int counter = 0;
        while (iter.hasNext())
        {
            Assert.assertNotNull(iter.next());
            ++counter;
        }

        Assert.assertEquals(2, counter);

        Assert.assertNotNull(proxySet.toArray());
        Assert.assertEquals(2, proxySet.toArray().length);
        Assert.assertEquals(2, proxySet.size());

        proxySet.remove(p);
        Assert.assertNotNull(proxySet.getDataCollection());
        Assert.assertEquals(1, proxySet.size());

        proxySet.removeAll(subaddresses);
        Assert.assertNotNull(proxySet.getDataCollection());
        Assert.assertTrue(((HashSet)proxySet.getDataCollection()).isEmpty());

        proxySet.clear();
        Assert.assertTrue(proxySet.isEmpty());

    }

    @After
    public void tearDown()
    {

    }

}
