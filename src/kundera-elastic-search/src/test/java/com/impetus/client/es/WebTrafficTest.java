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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author vivek.mishra
 * 
 *         junit to demonstrate composite key implementation.
 * 
 */
public class WebTrafficTest
{
    private static Node node = null;

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Builder builder = Settings.settingsBuilder();
        builder.put("path.home", "target/data");
        node = new NodeBuilder().settings(builder).node();
    }

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory("es-pu");
        em = emf.createEntityManager();
    }

    @Test
    public void test() throws InterruptedException
    {
        WebTrafficCompositeKey compositeKey = new WebTrafficCompositeKey();
        compositeKey.setIpAddress("192.168.112.176");
        compositeKey.setLogtime("2013-01-23");
        compositeKey.setUrl("www.google.com");

        WebTraffic traffic = new WebTraffic();
        traffic.setKey(compositeKey);
        traffic.setCountry("India");
        em.persist(traffic);

        waitThread();
        
        em.clear();
        
        WebTraffic result = em.find(WebTraffic.class, compositeKey);

        Assert.assertNotNull(result);

        Assert.assertNotNull(result.getKey());

        em.remove(result);

        em.clear();
        result = em.find(WebTraffic.class, compositeKey);
        Assert.assertNull(result);
    }

    private void waitThread() throws InterruptedException
    {
        Thread.sleep(2000);
    }

    @After
    public void tearDown()
    {
        if (em != null)
        {
            em.close();
        }

        if (emf != null)
        {
            emf.close();
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        node.close();
    }

}
