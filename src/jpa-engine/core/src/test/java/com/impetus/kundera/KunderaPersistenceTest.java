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
package com.impetus.kundera;

import javax.persistence.EntityManagerFactory;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author vivek.mishra
 * junit for KunderaPersistence.
 *
 */
public class KunderaPersistenceTest
{

    @Test
    public void testCreateEMFWithNullPu()
    {
        KunderaPersistence persistence = new KunderaPersistence();
        try
        {
            EntityManagerFactory emf = persistence.createEntityManagerFactory(null, null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (KunderaException kex)
        {
            Assert.assertEquals("Persistence unit name should not be null", kex.getMessage());
        }

    }

    @Test
    public void testCreateEMFWithPU()
    {
        KunderaPersistence persistence = new KunderaPersistence();
        try
        {
            EntityManagerFactory emf = persistence.createEntityManagerFactory("patest", null);
            Assert.assertNotNull(emf);
            Assert.assertTrue(emf.getClass().isAssignableFrom(EntityManagerFactoryImpl.class)); // emf should get created.
            
            Assert.assertNotNull(persistence.getProviderUtil());  // Assert on provider util.
            Assert.assertNotNull(persistence.getCache());         // Assert on cache.
        }
        catch (KunderaException kex)
        {
            Assert.fail();
        }

    }

}
