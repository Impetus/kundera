/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.kundera.client.cassandra.thrift;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.thrift.ThriftClientFactory;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.service.Host;

/**
 * The Class ThriftClientFactoryTest.
 * 
 * @author: karthikp.manchala
 */
public class ThriftClientFactoryTest
{

    /** The Constant PU. */
    private static final String PU = "cassandra_pool";

    /** The emf. */
    private EntityManagerFactory emf;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {

        emf = Persistence.createEntityManagerFactory(PU);
        emf.createEntityManager();

    }

    /**
     * Test.
     * 
     * @throws SecurityException
     *             the security exception
     * @throws NoSuchMethodException
     *             the no such method exception
     * @throws IllegalArgumentException
     *             the illegal argument exception
     * @throws IllegalAccessException
     *             the illegal access exception
     * @throws InvocationTargetException
     *             the invocation target exception
     * @throws NoSuchFieldException
     *             the no such field exception
     */
    @Test
    public void test() throws SecurityException, NoSuchMethodException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException, NoSuchFieldException
    {
        ThriftClientFactory thriftFactory = new ThriftClientFactory();

        Field f = thriftFactory.getClass().getSuperclass().getSuperclass().getDeclaredField("persistenceUnit");

        f.setAccessible(true);
        f.set(thriftFactory, PU);

        Method m = GenericClientFactory.class.getDeclaredMethod("setKunderaMetadata", KunderaMetadata.class);
        m.setAccessible(true);

        m.invoke(thriftFactory, ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
        thriftFactory.load(PU, null);

        Field hostPools = (thriftFactory.getClass().getSuperclass().getSuperclass()).getDeclaredField("hostPools");
        hostPools.setAccessible(true);
        ConcurrentMap<Host, Object> hosts = (ConcurrentMap<Host, Object>) hostPools.get(thriftFactory);

        // for each host check if pooling properties are applied
        for (Host host : hosts.keySet())
        {
            Assert.assertEquals(host.getMaxActive(), 200);
            Assert.assertEquals(host.getMaxIdle(), 200);
            Assert.assertEquals(host.getMinIdle(), 20);
        }
        thriftFactory.destroy();
    }

}
