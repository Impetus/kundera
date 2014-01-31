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
package com.impetus.kundera.service;

import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author vivek.mishra
 * 
 *         junit test case for HostConfiguration.
 * 
 */
public class HostconfigurationTest
{
    /* persistence unit. */
    private String persistenceUnit = "metaDataTest";

    private EntityManagerFactoryImpl emf;

    /**
     * on setup
     */
    @Before
    public void setUp()
    {
        emf = getEntityManagerFactory("create");
    }

    /**
     * test method.
     */
    @Test
    public void test()
    {
        CoreHostConfiguration hostConfiguration = new CoreHostConfiguration(null, null, persistenceUnit, emf.getKunderaMetadataInstance());
        Assert.assertNotNull(hostConfiguration.hosts);
        Assert.assertNotNull(hostConfiguration.port);
        Assert.assertTrue(hostConfiguration.getHosts().isEmpty());
        Assert.assertEquals("localhost", hostConfiguration.hosts);
        Assert.assertEquals("9160", hostConfiguration.port);

        try
        {
            hostConfiguration.onValidation(null, null);
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex.getMessage());
        }

    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(final String schemaProperty)
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory(persistenceUnit);
    }

}
