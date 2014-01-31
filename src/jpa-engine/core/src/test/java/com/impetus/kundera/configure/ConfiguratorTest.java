/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.configure;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * junit test case for {@link Configurator}.
 * 
 * @author vivek.mishra
 */
public class ConfiguratorTest
{
    private final String _persistenceUnit = "kunderatest";

    private final String kundera_client = "com.impetus.kundera.client.CoreTestClientFactory";

    private String _keyspace = "kunderatest";

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {

    }

    /**
     * Test valid configure.
     */
    @Test
    public void testValidConfigure()
    {
        // invoke configure.
       EntityManagerFactoryImpl emfImpl = getEntityManagerFactory();


        // Assert entity metadata
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(emfImpl.getKunderaMetadataInstance(), PersonnelDTO.class);
        Assert.assertNotNull(m);
        Assert.assertNotNull(m.getPersistenceUnit());
        Assert.assertEquals(_persistenceUnit, m.getPersistenceUnit());
        Assert.assertEquals(PersonnelDTO.class.getName(), m.getEntityClazz().getName());

        // Assert on persistence unit meta data.
        PersistenceUnitMetadata puMetadata = emfImpl.getKunderaMetadataInstance().getApplicationMetadata().getPersistenceUnitMetadata(
                _persistenceUnit);
        Assert.assertEquals(kundera_client, puMetadata.getClient());
        Assert.assertEquals(true, puMetadata.getExcludeUnlistedClasses());
        Assert.assertNotNull(puMetadata.getPersistenceUnitRootUrl());
        // emf.close();
    }

    // @Test
    public void testEntityListener()
    {
        EntityManagerFactory emf = getEntityManagerFactory();
        EntityManager em = emf.createEntityManager();
        PersonnelDTO dto = new PersonnelDTO();
        dto.setFirstName("Vivek");
        dto.setLastName("vivek");
        dto.setPersonId("1_p");
        em.persist(dto);
        PersonnelDTO result = em.find(PersonnelDTO.class, "1_p");
        Assert.assertNotNull(result);
        Assert.assertEquals("Mishra", result.getLastName());
        emf.close();

    }

    /**
     * Test invalid configure.
     */
    @Test
    public void testInvalidConfigure()
    {
        final String invalidPuName = "invalid";
        PersistenceUnitMetadata puMetadata = null;
        try
        {
            EntityManagerFactoryImpl emf = getEntityManagerFactory();
            puMetadata = emf.getKunderaMetadataInstance().getApplicationMetadata().getPersistenceUnitMetadata(invalidPuName);
        }
        catch (PersistenceUnitConfigurationException iex)
        {
            Assert.assertNull(puMetadata);
        }
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {

    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory("kunderatest");
    }
    /* *//**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    /*
     * private EntityManagerFactory getEntityManagerFactory() { return
     * Persistence.createEntityManagerFactory(_persistenceUnit); }
     */
}
