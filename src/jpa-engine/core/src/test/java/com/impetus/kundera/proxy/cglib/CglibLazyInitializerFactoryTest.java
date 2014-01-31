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
package com.impetus.kundera.proxy.cglib;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.LazyInitializer;
import com.impetus.kundera.proxy.LazyInitializerFactory;

/**
 * @author amresh.singh
 */
public class CglibLazyInitializerFactoryTest
{

    private static EntityManagerFactory emf;

    private static EntityManager em;
    
    private static KunderaMetadata kunderaMetadata;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        
        emf = Persistence.createEntityManagerFactory("kunderatest");
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
        em = emf.createEntityManager();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        em.close();
        emf.close();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {

    }

    /**
     * Test method for
     * {@link com.impetus.kundera.proxy.cglib.CglibLazyInitializerFactory#getProxy(java.lang.String, java.lang.Class, java.lang.reflect.Method, java.lang.reflect.Method, java.lang.Object, com.impetus.kundera.persistence.PersistenceDelegator)}
     * .
     */
    @Test
    public void testGetProxy()
    {

        LazyInitializerFactory factory = kunderaMetadata.getCoreMetadata().getLazyInitializerFactory();
        KunderaProxy proxy = factory.getProxy("personnel", PersonnelDTO.class, null, null, "1", null);
        LazyInitializer li = proxy.getKunderaLazyInitializer();
        Assert.assertEquals(CglibLazyInitializer.class, li.getClass());
        Assert.assertTrue(li.isUninitialized());
        Assert.assertFalse(li.isUnwrap());
        Assert.assertEquals("personnel", li.getEntityName());
        Assert.assertEquals("1", li.getIdentifier());
        Assert.assertNull(li.getOwner());
        Assert.assertNull(li.getPersistenceDelegator());
        Assert.assertEquals(PersonnelDTO.class, li.getPersistentClass());

    }

}
