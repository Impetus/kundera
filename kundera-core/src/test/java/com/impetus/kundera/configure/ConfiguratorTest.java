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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.EntityListenersProcessor;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * junit test case for {@link Configurator}.
 * 
 * @author vivek.mishra
 */
public class ConfiguratorTest
{
    private final String _persistenceUnit = "kunderatest";

    private final String kundera_client = "com.impetus.kundera.cache.ehcache.CoreTestClientFactory";

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
        // Configurator configurator = new Configurator(puName);
        // configurator.configure();
        getEntityManagerFactory();

        new PersistenceUnitConfiguration(_persistenceUnit).configure();
        // new MetamodelConfiguration(puName).configure();

        // Assert entity metadata
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(PersonnelDTO.class);
        Assert.assertNotNull(m);
        Assert.assertNotNull(m.getPersistenceUnit());
        Assert.assertEquals(_persistenceUnit, m.getPersistenceUnit());
        Assert.assertEquals(PersonnelDTO.class.getName(), m.getEntityClazz().getName());

        // Assert on persistence unit meta data.
        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(_persistenceUnit);
        Assert.assertEquals(kundera_client, puMetadata.getClient());
        Assert.assertEquals(true, puMetadata.getExcludeUnlistedClasses());
        Assert.assertNotNull(puMetadata.getPersistenceUnitRootUrl());
    }

/*    @Test
    public void testEntityListener()
    {
        getEntityManagerFactory();
        EntityListenersProcessor listener = new EntityListenersProcessor();
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(PersonnelDTO.class);
        listener.process(PersonnelDTO.class, m);
        
    }
*/    
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

            // invoke configure.
            Configurator configurator = new Configurator(null,invalidPuName);
            configurator.configure();
            puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(invalidPuName);
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
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(_persistenceUnit);
        clazzToPu.put(PersonnelDTO.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(PersonnelDTO.class);

        TableProcessor processor = new TableProcessor(null);
        processor.process(PersonnelDTO.class, m);

        m.setPersistenceUnit(_persistenceUnit);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(PersonnelDTO.class, m);

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(_persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(_persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(_persistenceUnit).getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(_persistenceUnit, metaModel);
        return null;
    }
}
