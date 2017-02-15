/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.client.kudu.embeddables;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;

/**
 * The Class KuduEmbeddableIdTest.
 */
public class KuduEmbeddableIdTest
{
    /** The Constant KUDU_PU. */
    private static final String KUDU_PU = "kudu";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(KUDU_PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * Test CRUD.
     */
    @Test
    public void testCRUD()
    {
        testInsert();
        testMerge();
        testDelete();
    }

    /**
     * Test insert.
     */
    private void testInsert()
    {

        Metrics metrics = new Metrics();
        metrics.setId(getMetricsId());
        metrics.setValue(100.0);

        em.persist(metrics);
        em.clear();

        Metrics m = em.find(Metrics.class, getMetricsId());

        Assert.assertNotNull(m);
        Assert.assertEquals(100.0, m.getValue());
        Assert.assertNotNull(m.getId());
        Assert.assertEquals("192.xx.xx.xx", m.getId().getHost());
        Assert.assertEquals("some info", m.getId().getMetric());
        Assert.assertEquals(1487142529001l, m.getId().getTime());
    }

    /**
     * Test merge.
     */
    private void testMerge()
    {

        Metrics m = em.find(Metrics.class, getMetricsId());
        m.setValue(200.0);

        em.merge(m);
        em.clear();
        Metrics m1 = em.find(Metrics.class, getMetricsId());
        Assert.assertNotNull(m1);
        Assert.assertEquals(200.0, m1.getValue());
        Assert.assertNotNull(m1.getId());
        Assert.assertEquals("192.xx.xx.xx", m1.getId().getHost());
        Assert.assertEquals("some info", m1.getId().getMetric());
        Assert.assertEquals(1487142529001l, m1.getId().getTime());
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        Metrics m = em.find(Metrics.class, getMetricsId());
        em.remove(m);
        em.clear();
        Metrics m1 = em.find(Metrics.class, getMetricsId());
        Assert.assertNull(m1);
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (emf != null)
        {
            emf.close();
        }
    }

    /**
     * Gets the metrics id.
     *
     * @return the metrics id
     */
    private MetricsId getMetricsId()
    {
        return new MetricsId("192.xx.xx.xx", "some info", 1487142529001l);
    }

}
