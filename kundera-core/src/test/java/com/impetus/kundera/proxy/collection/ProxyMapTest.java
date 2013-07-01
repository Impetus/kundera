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

import java.util.HashSet;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FetchType;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.event.AddressEntity;

/**
 * @author vivek.mishra
 * junit for {@link ProxyMap}
 */
public class ProxyMapTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    @Before
    public void setup()
    {

        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
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
        ProxyMap proxyMap = new ProxyMap(delegator, relation);

        proxyMap.setOwner(p);

        
        Assert.assertEquals(p, proxyMap.getOwner());
        Assert.assertNull(proxyMap.getDataCollection());
        Assert.assertNotNull(proxyMap.getRelation());
        Assert.assertEquals(relation, proxyMap.getRelation());
        Assert.assertEquals(delegator, proxyMap.getPersistenceDelegator());
        Assert.assertNotNull(proxyMap.getCopy());
        Assert.assertEquals(proxyMap.getRelation(),proxyMap.getCopy().getRelation());

        //TODO: This needs to be properly tested with Map relation. Need to discuss with Amresh.
    }

}
