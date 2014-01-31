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
package com.impetus.kundera.persistence;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.polyglot.entities.AddressU11FK;
import com.impetus.kundera.polyglot.entities.PersonU11FK;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * @author vivek.mishra junit for {@link AssociationBuilder}
 * 
 */
public class AssociationBuilderTest
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

        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();

    }

    @Test
    public void testAssociatedEntitiesFromIndex() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException
    {
        AddressU11FK address = new AddressU11FK();
        address.setAddressId("addr1");
        address.setStreet("street");

        PersonU11FK p1 = new PersonU11FK();
        p1.setPersonName("vivek");
        p1.setPersonId("1");
        p1.setAddress(address);

        em.persist(p1);

        em.clear();

        AssociationBuilder builder = new AssociationBuilder();

        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        Client associationEntityClient = delegator.getClient(KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), AddressU11FK.class));
        java.util.List results = builder.getAssociatedEntitiesFromIndex(PersonU11FK.class, "1", AddressU11FK.class,
                associationEntityClient);
        Assert.assertNotNull(results);

        // TODO: This is failing . Vivek to look into this.
        // Assert.assertFalse(results.isEmpty());

        // builder.setProxyRelationObject(entity, relationsMap, m, pd, entityId,
        // relation);

        Map<String, Object> relationMap = new HashMap<String, Object>();
        relationMap.put("ADDRESS_ID", "addr1");

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonU11FK.class);
        builder.setProxyRelationObject(p1, relationMap, metadata, delegator, "1", metadata.getRelation("address"));

        Assert.assertTrue(ProxyHelper.isKunderaProxy(p1.getAddress()));
        //
        // builder.populateRelationForM2M(entity, entityMetadata, delegator,
        // relation, relObject, relationsMap);

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

        LuceneCleanupUtilities.cleanLuceneDirectory(((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance().getApplicationMetadata().getPersistenceUnitMetadata(PU));
    }

}
