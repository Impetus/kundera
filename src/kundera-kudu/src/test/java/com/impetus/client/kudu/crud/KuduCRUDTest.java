/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu.crud;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.kudu.entities.Person;

import junit.framework.Assert;

/**
 * The Class KuduCRUDTest.
 */
public class KuduCRUDTest
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
        Person person = new Person("101", "dev", 22, 30000.5);
        em.persist(person);
        em.clear();
        Person p = em.find(Person.class, "101");
        Assert.assertNotNull(p);
        Assert.assertEquals("101", p.getPersonId());
        Assert.assertEquals("dev", p.getPersonName());
        Assert.assertEquals(22, p.getAge());
        Assert.assertEquals(30000.5, p.getSalary());
    }

    /**
     * Test merge.
     */
    private void testMerge()
    {
        Person p = em.find(Person.class, "101");
        p.setPersonName("karthik");
        em.merge(p);
        em.clear();
        Person p1 = em.find(Person.class, "101");
        Assert.assertNotNull(p1);
        Assert.assertEquals("karthik", p1.getPersonName());
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        Person p = em.find(Person.class, "101");
        em.remove(p);
        em.clear();
        Person p1 = em.find(Person.class, "101");
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
