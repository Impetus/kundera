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
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.Client;

/**
 * @author vivek.mishra
 * junit for {@link ESClientPropertyReader} 
 */
public class ESClientPropertyReaderTest
{

    private static final String PERSISTENCE_UNIT = "es-external-config";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        em = emf.createEntityManager();
    }

    @Test
    public void test() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        Map<String,Client<ESQuery>> clients = (Map<String, Client<ESQuery>>) em.getDelegate();
        
        ESClient client = (ESClient) clients.get(PERSISTENCE_UNIT);
        
        Field factoryField = client.getClass().getDeclaredField("factory");
        
        if(!factoryField.isAccessible())
        {
            factoryField.setAccessible(true);
        }
        
        ESClientFactory factory = (ESClientFactory) factoryField.get(client);
        
        
        Field propertyReader = ((ESClientFactory) factory).getClass().getSuperclass().getDeclaredField("propertyReader");

        if (!propertyReader.isAccessible())
        {
            propertyReader.setAccessible(true);
        }

        ESClientPropertyReader readerInstance = (ESClientPropertyReader) propertyReader.get(factory);
        Properties props = readerInstance.getConnectionProperties();

        Assert.assertEquals("true", props.get("client.transport.sniff"));
        Assert.assertEquals("false", props.get("discovery.zen.ping.multicast.enabled"));
        Assert.assertEquals("true", props.get("discovery.zen.ping.unicast.enabled"));
        Assert.assertEquals("false", props.get("discovery.zen.multicast.enabled"));
        Assert.assertEquals("true", props.get("discovery.zen.unicast.enabled"));
    }

}
