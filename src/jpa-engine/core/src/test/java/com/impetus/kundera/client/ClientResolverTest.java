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
package com.impetus.kundera.client;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.loader.ClientFactory;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author vivek.mishra
 * 
 *         junit for {@link ClientResolver}
 * 
 */
public class ClientResolverTest
{
    private final String persistenceUnit = "patest";

    @Test
    public void test()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit);
        ClientResolver.getClientFactory(persistenceUnit, null,
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
        ClientFactory clientFactory = ClientResolver.getClientFactory(persistenceUnit);
        Assert.assertNotNull(clientFactory);
        Assert.assertTrue(clientFactory.getClass().isAssignableFrom(CoreTestClientFactory.class));
    }

    @Test
    public void testInvalidPU()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit);
        ClientResolver.getClientFactory(persistenceUnit, null,
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
        try
        {
            ClientResolver.getClientFactory("metadatatest");
            Assert.fail("Should have gone to catch block!");
        }
        catch (ClientResolverException crex)
        {
            Assert.assertEquals("Client Factory Not Configured For Specified Client Type.", crex.getMessage());
        }
    }
}
