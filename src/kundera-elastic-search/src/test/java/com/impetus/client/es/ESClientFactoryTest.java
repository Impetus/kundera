/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.es;

import java.lang.reflect.Field;
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
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * @author vivek.mishra junit for {@link ESClientFactory}
 * 
 */
public class ESClientFactoryTest
{
    private final static String persistenceUnit = "es-pu";

    private EntityManagerFactory emf;

    @Before
    public void setup()
    {
        emf = getEntityManagerFactory();
    }

    @Test
    public void test() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        ESClientFactory esFactory = new ESClientFactory();
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9300");

        Field f = esFactory.getClass().getSuperclass().getDeclaredField("persistenceUnit");

        if (!f.isAccessible())
        {
            f.setAccessible(true);
        }
        f.set(esFactory, persistenceUnit);
        Method m = GenericClientFactory.class.getDeclaredMethod("setKunderaMetadata", KunderaMetadata.class);
        if (!m.isAccessible())
        {
            m.setAccessible(true);
        }

        m.invoke(esFactory, ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());

        esFactory.initialize(props);

        Assert.assertNotNull(esFactory.getClientInstance());
    }

    private EntityManagerFactory getEntityManagerFactory()
    {
        return Persistence.createEntityManagerFactory("es-pu");
    }

}
