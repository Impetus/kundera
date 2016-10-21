/*******************************************************************************
 * * Copyright 2016 Impetus Infotech.
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
package com.impetus.kundera;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.entities.Person;

/**
 * The Class CRUDTest.
 */
public class CRUDTest
{

    /** The Constant PU. */
    private static final String PU = "cassandra_pu";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up before class.
     *
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
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
     * Test crud operations.
     *
     * @throws Exception
     *             the exception
     */
    @Test
    public void testCRUDOperations() throws Exception
    {
        testInsert();
        testMerge();
        testRemove();
    }

    /**
     * Test insert.
     *
     * @throws Exception
     *             the exception
     */
    private void testInsert() throws Exception
    {
        Person p = new Person();
        p.setPersonId("101");
        p.setPersonName("dev");
        p.setAge(24);
        em.persist(p);

        Person person = em.find(Person.class, "101");
        Assert.assertNotNull(person);
        Assert.assertEquals("101", person.getPersonId());
        Assert.assertEquals("dev", person.getPersonName());

    }

    /**
     * Test merge.
     */
    private void testMerge()
    {
        Person person = em.find(Person.class, "101");
        person.setPersonName("devender");
        em.merge(person);

        Person p1 = em.find(Person.class, "101");
        Assert.assertEquals("devender", p1.getPersonName());
    }

    /**
     * Test remove.
     */
    private void testRemove()
    {
        Person p = em.find(Person.class, "101");
        em.remove(p);

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
            emf = null;
        }
    }
}
