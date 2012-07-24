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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * junit test case for {@link Configurator}.
 * 
 * @author vivek.mishra
 */
public class ConfiguratorTest
{

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
        final String puName = "kunderatest";
        final String kundera_client = "com.impetus.client.rdbms.RDBMSClientFactory";

        // invoke configure.
        // Configurator configurator = new Configurator(puName);
        // configurator.configure();

        new PersistenceUnitConfiguration(puName).configure();
        new MetamodelConfiguration(puName).configure();

        // Assert entity metadata
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(PersonnelDTO.class);
        Assert.assertNotNull(m);
        Assert.assertNotNull(m.getPersistenceUnit());
        Assert.assertEquals(puName, m.getPersistenceUnit());
        Assert.assertEquals(PersonnelDTO.class.getName(), m.getEntityClazz().getName());

        // Assert on persistence unit meta data.
        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(puName);
        Assert.assertEquals(kundera_client, puMetadata.getClient());
        Assert.assertEquals(false, puMetadata.getExcludeUnlistedClasses());
        Assert.assertNotNull(puMetadata.getPersistenceUnitRootUrl());
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

            // invoke configure.
            Configurator configurator = new Configurator(invalidPuName);
            configurator.configure();
            puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(invalidPuName);
        }
        catch (IllegalArgumentException iex)
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

}
