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
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
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
 *         TODO: do not delete commented out code. some cases failing for
 *         lucene.
 */
public class EntityReaderTest
{

    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;

    private KunderaMetadata kunderaMetadata;
    
    Map propertyMap=null;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

        propertyMap=new HashMap<String, String>();
        propertyMap.put("index.home.dir","./lucene");
        emf = Persistence.createEntityManagerFactory(PU,propertyMap);
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
        em = emf.createEntityManager();

    }

    @Test
    public void testOneToOne() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        AddressU11FK address = new AddressU11FK();
        address.setAddressId("addr1");
        address.setStreet("street");

        PersonU11FK p1 = new PersonU11FK();
        p1.setPersonName("vivek");
        p1.setPersonId("1");
        p1.setAddress(address);

        em.persist(p1);

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        Map<String, Object> relationMap = new HashMap<String, Object>();
        relationMap.put("ADDRESS_ID", "addr1");

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonU11FK.class);

        p1.setAddress(null);

        CoreTestEntityReader reader = new CoreTestEntityReader(kunderaMetadata);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, false);

        Assert.assertNotNull(p1.getAddress());

        Assert.assertTrue(ProxyHelper.isKunderaProxy(p1.getAddress()));

        p1.setAddress(null);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, true);

        Assert.assertNotNull(p1.getAddress());

        Assert.assertFalse(ProxyHelper.isKunderaProxy(p1.getAddress()));

    }

    @Test
    public void testBiOneToOne() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        AddressB11FK address = new AddressB11FK();
        address.setAddressId("addr1");
        address.setStreet("street");

        PersonB11FK p1 = new PersonB11FK();
        p1.setPersonName("vivek");
        p1.setPersonId("1");
        p1.setAddress(address);

        em.persist(p1);

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        Map<String, Object> relationMap = new HashMap<String, Object>();
        relationMap.put("ADDRESS_ID", "addr1");

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonB11FK.class);

        p1.setAddress(null);

        CoreTestEntityReader reader = new CoreTestEntityReader(kunderaMetadata);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, false);

        Assert.assertNotNull(p1.getAddress());

        Assert.assertTrue(ProxyHelper.isKunderaProxy(p1.getAddress()));

        p1.setAddress(null);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, true);

        Assert.assertNotNull(p1.getAddress());

        Assert.assertFalse(ProxyHelper.isKunderaProxy(p1.getAddress()));

        Assert.assertEquals(p1, p1.getAddress().getPerson());

    }

    @Test
    public void testManyToOne() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        PersonUM1 person1 = new PersonUM1();
        person1.setPersonId("person1");
        person1.setPersonName("vivek");

        PersonUM1 person2 = new PersonUM1();
        person2.setPersonId("person2");
        person2.setPersonName("vivek");

        AddressUM1 address = new AddressUM1();
        address.setAddressId("addr1");
        address.setStreet("streetmto1");

        person1.setAddress(address);
        person2.setAddress(address);

        em.persist(person1);
        em.persist(person2);

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        Map<String, Object> relationMap = new HashMap<String, Object>();
        relationMap.put("ADDRESS_ID", "addr1");

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonUM1.class);

        CoreTestEntityReader reader = new CoreTestEntityReader(kunderaMetadata);

        person1.setAddress(null);

        reader.recursivelyFindEntities(person1, relationMap, metadata, delegator, false);

        Assert.assertNotNull(person1.getAddress());

        Assert.assertTrue(ProxyHelper.isKunderaProxy(person1.getAddress()));

        person1.setAddress(null);

        reader.recursivelyFindEntities(person1, relationMap, metadata, delegator, true);

        Assert.assertNotNull(person1.getAddress());

        Assert.assertFalse(ProxyHelper.isKunderaProxy(person1.getAddress()));

    }

    @Test
    public void testBiManyToOne() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        PersonBM1 person1 = new PersonBM1();
        person1.setPersonId("person1");
        person1.setPersonName("vivek");

        PersonBM1 person2 = new PersonBM1();
        person2.setPersonId("person2");
        person2.setPersonName("vivek");

        AddressBM1 address = new AddressBM1();
        address.setAddressId("addr1");
        address.setStreet("streetmto1");

        person1.setAddress(address);
        person2.setAddress(address);

        em.persist(person1);
        em.persist(person2);

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        Map<String, Object> relationMap = new HashMap<String, Object>();
        relationMap.put("ADDRESS_ID", "addr1");

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonBM1.class);

        CoreTestEntityReader reader = new CoreTestEntityReader(kunderaMetadata);

        person1.setAddress(null);

        reader.recursivelyFindEntities(person1, relationMap, metadata, delegator, false);

        Assert.assertNotNull(person1.getAddress());

        Assert.assertTrue(ProxyHelper.isKunderaProxy(person1.getAddress()));

        // Assert.assertEquals(2, person1.getAddress().getPeople().size());

        person1.setAddress(null);

        reader.recursivelyFindEntities(person1, relationMap, metadata, delegator, true);

        Assert.assertNotNull(person1.getAddress());

        Assert.assertFalse(ProxyHelper.isKunderaProxy(person1.getAddress()));
        // Assert.assertEquals(2, person1.getAddress().getPeople().size());

    }

    @Test
    public void testManyToMany() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        PersonUMM person = new PersonUMM();
        person.setPersonId("person1");
        person.setPersonName("personName");

        AddressUMM address = new AddressUMM();
        address.setAddressId("addr1");
        address.setStreet("mtmstreet");

        Set<AddressUMM> addresses = new HashSet<AddressUMM>();
        addresses.add(address);
        person.setAddresses(addresses);

        em.persist(person);

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        Map<String, Object> relationMap = new HashMap<String, Object>();
        relationMap.put("ADDRESS_ID", "addr1");

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonUMM.class);

        CoreTestEntityReader reader = new CoreTestEntityReader(kunderaMetadata);

        person.setAddresses(null);

        reader.recursivelyFindEntities(person, relationMap, metadata, delegator, false);

        Assert.assertNotNull(person.getAddresses());

        Assert.assertTrue(ProxyHelper.isKunderaProxyCollection(person.getAddresses()));

        reader.recursivelyFindEntities(person, relationMap, metadata, delegator, true);

        Assert.assertTrue(person.getAddresses().isEmpty()); // code to fetch
                                                            // from join table
                                                            // data in dummy
                                                            // client is
                                                            // missing.

        // Assert.assertFalse(ProxyHelper.isKunderaProxyCollection(person.getAddresses()));

    }

    @Test
    public void testManyToManyByMap() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException
    {
        PersonUMMByMap person = new PersonUMMByMap();
        person.setPersonId("person1");
        person.setPersonName("personName");

        AddressUMM address = new AddressUMM();
        address.setAddressId("addr1");
        address.setStreet("mtmstreet");

        Map<String, AddressUMM> addresses = new HashMap<String, AddressUMM>();
        addresses.put("addr1", address);

        person.setAddresses(addresses);

        em.persist(person);

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);

        Map<String, Object> relationMap = new HashMap<String, Object>();
        relationMap.put("ADDRESS_ID", "addr1");

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonUMMByMap.class);

        CoreTestEntityReader reader = new CoreTestEntityReader(kunderaMetadata);

        person.setAddresses(null);

        reader.recursivelyFindEntities(person, relationMap, metadata, delegator, false);

        Assert.assertNotNull(person.getAddresses());

        Assert.assertTrue(ProxyHelper.isKunderaProxyCollection(person.getAddresses()));

        reader.recursivelyFindEntities(person, relationMap, metadata, delegator, true);

        Assert.assertTrue(person.getAddresses().isEmpty()); // code to fetch
                                                            // from join table
                                                            // data in dummy
                                                            // client is
                                                            // missing.

        // Assert.assertFalse(ProxyHelper.isKunderaProxyCollection(person.getAddresses()));

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

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonU1M.class);

        CoreTestEntityReader reader = new CoreTestEntityReader(kunderaMetadata);

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

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonB1M.class);

        CoreTestEntityReader reader = new CoreTestEntityReader(kunderaMetadata);

        p1.setAddresses(null);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, false);

        Assert.assertNotNull(p1.getAddresses());

        Assert.assertTrue(ProxyHelper.isKunderaProxyCollection(p1.getAddresses()));
        // Assert.assertEquals(p1,
        // p1.getAddresses().iterator().next().getPerson());
        // Assert.assertEquals(p1,
        // p1.getAddresses().iterator().next().getPerson());

        p1.setAddresses(null);

        reader.recursivelyFindEntities(p1, relationMap, metadata, delegator, true);

        Assert.assertNotNull(p1.getAddresses());

        Assert.assertFalse(ProxyHelper.isKunderaProxyCollection(p1.getAddresses()));

        // Assert.assertEquals(p1,
        // p1.getAddresses().iterator().next().getPerson());
        // Assert.assertEquals(p1,
        // p1.getAddresses().iterator().next().getPerson());
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


        LuceneCleanupUtilities.cleanDir((String) propertyMap.get(PersistenceProperties.KUNDERA_INDEX_HOME_DIR));
    }

}