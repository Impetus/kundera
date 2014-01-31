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

import com.impetus.kundera.CoreTestUtilities;
import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.LazyInitializationException;
import com.impetus.kundera.proxy.LazyInitializer;
import com.impetus.kundera.proxy.LazyInitializerFactory;

/**
 * @author amresh.singh
 */
public class CglibLazyInitializerTest
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
    public void testWithNullPD()
    {

        LazyInitializerFactory factory = kunderaMetadata.getCoreMetadata().getLazyInitializerFactory();
        KunderaProxy proxy = factory.getProxy("personnel", PersonnelDTO.class, null, null, "1", null);
        LazyInitializer li = proxy.getKunderaLazyInitializer();
        Assert.assertEquals(CglibLazyInitializer.class, li.getClass());
        
        try
        {
            li.initialize();
        }catch(LazyInitializationException liex)
        {
//            Assert.assertEquals("could not initialize proxy " + PersonnelDTO.class.getName() + "_"
//                        + "1" + " - no EntityManager", liex.getMessage());
        }

    }

    @Test
    public void testWithPDInstance() throws NoSuchMethodException, Throwable
    {
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        PersonnelDTO dto = new PersonnelDTO("1", "vivek", "mishra");
        em.persist(dto);
        LazyInitializerFactory factory = kunderaMetadata.getCoreMetadata().getLazyInitializerFactory();
        KunderaProxy proxy = factory.getProxy("personnel#1", PersonnelDTO.class, null, null, "1", delegator);
        LazyInitializer li = proxy.getKunderaLazyInitializer();
        ((CglibLazyInitializer)li).setPersistenceDelegator(delegator);
        li.setImplementation(proxy);
        
        li.initialize();
        Assert.assertNotNull(((CglibLazyInitializer)li).getTarget());
        Assert.assertNotNull(((CglibLazyInitializer)li).getEntityName());
        Assert.assertEquals("personnel#1",((CglibLazyInitializer)li).getEntityName());
        Assert.assertNotNull(li.getPersistenceDelegator());
        Assert.assertSame(delegator,li.getPersistenceDelegator());
        
        Assert.assertFalse(li.isUninitialized());
        
        Assert.assertSame(PersonnelDTO.class,li.getPersistentClass());
        Assert.assertEquals("1",li.getIdentifier());
        
        li.setIdentifier("12");
        Assert.assertEquals("12", li.getIdentifier());
        Assert.assertNotNull(li.getImplementation());
        
//        Object firstName = ((CglibLazyInitializer)li).invoke(proxy, dto.getClass().getDeclaredMethod("getFirstName", null),new String[]{});
//        Assert.assertEquals("vivek", firstName);
        
        ((CglibLazyInitializer)li).unsetPersistenceDelegator();
        ((CglibLazyInitializer)li).setUnwrap(true);
        Assert.assertTrue(((CglibLazyInitializer)li).isUnwrap());


    }
    
    @Test
    public void testWithClosedPDInstance() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        PersistenceDelegator delegator = CoreTestUtilities.getDelegator(em);
        PersonnelDTO dto = new PersonnelDTO("1", "vivek", "mishra");
        em.persist(dto);
        em.close();

        LazyInitializerFactory factory = kunderaMetadata.getCoreMetadata().getLazyInitializerFactory();
        KunderaProxy proxy = factory.getProxy("personnel", PersonnelDTO.class, null, null, "1", delegator);
        LazyInitializer li = proxy.getKunderaLazyInitializer();
        
        try
        {
            li.initialize();
            Assert.fail("Should have gone to catch block!");
        } catch(LazyInitializationException liex)
        {
            Assert.assertEquals("could not initialize proxy " + PersonnelDTO.class.getName() + "_"
                    + "1" + " - the owning Session was closed",liex.getMessage());
        }
        
        em = emf.createEntityManager();
    }
        
}
