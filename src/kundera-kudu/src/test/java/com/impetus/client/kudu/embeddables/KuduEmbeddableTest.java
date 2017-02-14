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

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class KuduEmbeddableTest
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
        Address addr = new Address();
        addr.setStreet("4th main");
        addr.setCity("Bangalore");
        addr.setCountry("India");
        EmbeddablePerson embeddablePerson = new EmbeddablePerson("101", "karthik", addr);
        em.persist(embeddablePerson);
        em.clear();
        EmbeddablePerson p = em.find(EmbeddablePerson.class, "101");
        Assert.assertNotNull(p);
        Assert.assertEquals("101", p.getPersonId());
        Assert.assertEquals("karthik", p.getPersonName());
        Assert.assertNotNull(p.getAddress());
        Assert.assertEquals("4th main", p.getAddress().getStreet());
        Assert.assertEquals("Bangalore", p.getAddress().getCity());
        Assert.assertEquals("India", p.getAddress().getCountry());
    }

    /**
     * Test merge.
     */
    private void testMerge()
    {
        EmbeddablePerson p = em.find(EmbeddablePerson.class, "101");
        p.setPersonName("kp");
        Address addr = new Address();
        addr.setStreet("Sector-59");
        addr.setCity("Noida");
        addr.setCountry("India");
        p.setAddress(addr);
        em.merge(p);
        em.clear();
        EmbeddablePerson p1 = em.find(EmbeddablePerson.class, "101");
        Assert.assertNotNull(p1);
        Assert.assertEquals("101", p.getPersonId());
        Assert.assertEquals("kp", p.getPersonName());
        Assert.assertNotNull(p.getAddress());
        Assert.assertEquals("Sector-59", p.getAddress().getStreet());
        Assert.assertEquals("Noida", p.getAddress().getCity());
        Assert.assertEquals("India", p.getAddress().getCountry());
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        EmbeddablePerson p = em.find(EmbeddablePerson.class, "101");
        em.remove(p);
        em.clear();
        EmbeddablePerson p1 = em.find(EmbeddablePerson.class, "101");
        Assert.assertNull(p1);
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

}
