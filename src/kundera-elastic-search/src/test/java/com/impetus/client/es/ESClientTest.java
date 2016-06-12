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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.es.PersonES.Day;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

public class ESClientTest
{
    private final static String persistenceUnit = "es-pu";

    private Node node = null;

    private EntityManagerFactory emf;

    @Before
    public void setup() throws Exception
    {
        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
        emf = getEntityManagerFactory();
    }

    @Test
    public void test() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException, InterruptedException, InvocationTargetException, NoSuchMethodException
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
        esFactory.load(persistenceUnit, props);

        ESClient client = (ESClient) esFactory.getClientInstance();

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonES.class);

        PersonES entity = new PersonES();
        entity.setAge(21);
        entity.setDay(Day.FRIDAY);
        entity.setPersonId("1");
        entity.setPersonName("vivek");
        client.onPersist(metadata, entity, "1", null);

        Thread.sleep(3000);
        PersonES result = (PersonES) client.find(PersonES.class, "1");
        Assert.assertNotNull(result);

        PersonES invalidResult = (PersonES) client.find(PersonES.class, "2_p");
        Assert.assertNull(invalidResult);

        client.delete(result, "1");
        result = (PersonES) client.find(PersonES.class, "1");
        Assert.assertNull(result);
    }

    private EntityManagerFactory getEntityManagerFactory()
    {
        return Persistence.createEntityManagerFactory("es-pu");
    }

    @After
    public void tearDown() throws Exception
    {
        if (checkIfServerRunning() && node != null)
        {
            node.close();
        }

    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9300);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}