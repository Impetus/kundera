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

package com.impetus.kundera.persistence;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.polyglot.entities.AddressB11FK;
import com.impetus.kundera.polyglot.entities.AddressB1M;
import com.impetus.kundera.polyglot.entities.AddressBM1;
import com.impetus.kundera.polyglot.entities.AddressU11FK;
import com.impetus.kundera.polyglot.entities.AddressU1M;
import com.impetus.kundera.polyglot.entities.AddressUM1;
import com.impetus.kundera.polyglot.entities.AddressUMM;
import com.impetus.kundera.polyglot.entities.PersonB11FK;
import com.impetus.kundera.polyglot.entities.PersonB1M;
import com.impetus.kundera.polyglot.entities.PersonBM1;
import com.impetus.kundera.polyglot.entities.PersonU11FK;
import com.impetus.kundera.polyglot.entities.PersonU1M;
import com.impetus.kundera.polyglot.entities.PersonUM1;
import com.impetus.kundera.polyglot.entities.PersonUMM;
import com.impetus.kundera.polyglot.entities.PersonUMMByMap;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.query.CoreTestEntityReader;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * @author vivek.mishra junit for {@link AbstractEntityReader}.
 * 
 * TODO: do not delete commented out code. some cases failing for lucene.
 */
public class EntityReaderTest
{

    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();

    }

    @Test
    public void testOneToMany() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        AddressU1M address1 = new AddressU1M();
        address1.setAddressId("addr1");
        address1.setStreet("street");

        AddressU1M address2 = new AddressU1M();
        address2.setAddressId("addr1");
        address2.setStreet("street");

        Set<AddressU1M> addressess = new HashSet<AddressU1M>();
        addressess.add(address1);
        addressess.add(address2);

        PersonU1M p1 = new PersonU1M();
        p1.setPersonName("vivek");
        p1.setPersonId("1");
        p1.setAddresses(addressess);

        em.persist(p1);

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        Map<String, Object> relationMap = null;

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(PersonU1M.class);

        CoreTestEntityReader reader = new CoreTestEntityReader();

        p1.setAddresses(null);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, false);

        Assert.assertNotNull(p1.getAddresses());

        Assert.assertTrue(ProxyHelper.isKunderaProxyCollection(p1.getAddresses()));

        p1.setAddresses(null);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, true);

        Assert.assertNotNull(p1.getAddresses());

        Assert.assertFalse(ProxyHelper.isKunderaProxyCollection(p1.getAddresses()));

    }

    @Test
    public void testBiOneToMany() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        AddressB1M address1 = new AddressB1M();
        address1.setAddressId("addr1");
        address1.setStreet("street");

        AddressB1M address2 = new AddressB1M();
        address2.setAddressId("addr1");
        address2.setStreet("street");

        Set<AddressB1M> addressess = new HashSet<AddressB1M>();
        addressess.add(address1);
        addressess.add(address2);

        PersonB1M p1 = new PersonB1M();
        p1.setPersonName("vivek");
        p1.setPersonId("1");
        p1.setAddresses(addressess);

        em.persist(p1);

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        Map<String, Object> relationMap = null;

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(PersonB1M.class);

        CoreTestEntityReader reader = new CoreTestEntityReader();

        p1.setAddresses(null);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, false);

        Assert.assertNotNull(p1.getAddresses());

        Assert.assertTrue(ProxyHelper.isKunderaProxyCollection(p1.getAddresses()));
//        Assert.assertEquals(p1, p1.getAddresses().iterator().next().getPerson());
//        Assert.assertEquals(p1, p1.getAddresses().iterator().next().getPerson());

        p1.setAddresses(null);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, true);

        Assert.assertNotNull(p1.getAddresses());

        Assert.assertFalse(ProxyHelper.isKunderaProxyCollection(p1.getAddresses()));
        
//        Assert.assertEquals(p1, p1.getAddresses().iterator().next().getPerson());
//        Assert.assertEquals(p1, p1.getAddresses().iterator().next().getPerson());
    }


    @After
    public void tearDown()
    {
        if (emf != null)
        {
            emf.close();
        }

        if (em != null)
        {
            em.close();
        }

        LuceneCleanupUtilities.cleanLuceneDirectory(PU);
    }

}
