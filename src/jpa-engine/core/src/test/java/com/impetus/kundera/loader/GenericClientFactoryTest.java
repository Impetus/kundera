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
package com.impetus.kundera.loader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.CoreTestClientFactory;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * @author vivek.mishra
 * junit for {@link GenericClientFactory}
 *
 */
public class GenericClientFactoryTest
{
    private static final String PU = "patest";

    private EntityManagerFactory emf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
                
        emf = Persistence.createEntityManagerFactory(PU);
        emf.createEntityManager();

    }

    @Test
    public void test() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        Map<String, PersistenceUnitMetadata> puMetadataMap = new HashMap<String, PersistenceUnitMetadata>();
        puMetadataMap.put(PU, puMetadata);
        ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance().getApplicationMetadata().addPersistenceUnitMetadata(puMetadataMap);
        CoreTestClientFactory clientFactory = new CoreTestClientFactory();
               
        
        Method m = GenericClientFactory.class.getDeclaredMethod("setKunderaMetadata", KunderaMetadata.class);
        if (!m.isAccessible())
        {
            m.setAccessible(true);
        }

        m.invoke(clientFactory, ((EntityManagerFactoryImpl)emf).getKunderaMetadataInstance());
        
        clientFactory.load(PU, null);
        
        clientFactory.setExternalProperties(new HashMap<String, Object>());
        
        Assert.assertEquals(PU,clientFactory.getPersistenceUnit());
        Assert.assertNotNull(clientFactory.getClientInstance());
        Assert.assertNotNull(clientFactory.getSchemaManager(null));
        Assert.assertNull(clientFactory.getConnectionPoolOrConnection());
        
        Assert.assertNotNull(clientFactory.getLoadBalancePolicy("ROUNDROBIN"));
        
        Assert.assertNotNull(clientFactory.getLoadBalancePolicy("LEASTACTIVE"));
        
        Assert.assertNotNull(clientFactory.getLoadBalancePolicy("invalid"));
        
        try
        {
            clientFactory.onValidation(null, null);
            Assert.fail("Should have gone to catch block!");
        }catch(IllegalArgumentException iaex)
        {
            Assert.assertNotNull(iaex.getMessage());
        }
        clientFactory.destroy();
    }

    @Test
    public void testIndexerClass()
    {
//        kunderaMetadata.addClientMetadata(PU, null);
        Map<String, Object> propertyMap = new HashMap<String, Object>();
        propertyMap.put(PersistenceProperties.KUNDERA_INDEXER_CLASS, "com.impetus.kundera.query.CoreIndexer");
        propertyMap.put(PersistenceProperties.KUNDERA_INDEX_HOME_DIR, "");
        propertyMap.put(PersistenceProperties.KUNDERA_AUTO_GENERATOR_CLASS, "");
        CoreTestClientFactory clientFactory = new CoreTestClientFactory();
        clientFactory.load(PU, propertyMap);
        
        Assert.assertNotNull(clientFactory.getClientMetadata());
    }
}
