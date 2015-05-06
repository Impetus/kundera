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
package com.impetus.client.crud;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.utils.MongoUtils;

/**
 * The Class MongoUniMTMTest.
 * 
 * @author Devender Yadav
 */
public class MongoUniMTMTest
{

    /** The Constant _PU. */
    private static final String _PU = "mongoTest";

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
        emf = Persistence.createEntityManagerFactory(_PU);
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
        MongoUtils.dropDatabase(emf, _PU);
        emf.close();
    }

    /**
     * Test CRUD for Many to Many.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testMTMCRUD() throws Exception
    {
        testInsert();
        testUpdate();
        testDelete();
    }

    /**
     * Test insert.
     */
    private void testInsert()
    {
        PersonMTM person1 = new PersonMTM();
        person1.setPersonId("p1");
        person1.setPersonName("Dev");

        PersonMTM person2 = new PersonMTM();
        person2.setPersonId("p2");
        person2.setPersonName("PG");

        AddressMTM address1 = new AddressMTM();
        address1.setAddressId("a1");
        address1.setStreet("AAAAAAAAAAAAA");

        AddressMTM address2 = new AddressMTM();
        address2.setAddressId("a2");
        address2.setStreet("BBBBBBBBBBBBBBB");

        AddressMTM address3 = new AddressMTM();
        address3.setAddressId("a3");
        address3.setStreet("CCCCCCCCCCC");

        Set<AddressMTM> person1Addresses = new HashSet<AddressMTM>();
        Set<AddressMTM> person2Addresses = new HashSet<AddressMTM>();

        person1Addresses.add(address1);
        person1Addresses.add(address2);

        person2Addresses.add(address2);
        person2Addresses.add(address3);

        person1.setAddresses(person1Addresses);
        person2.setAddresses(person2Addresses);

        em.persist(person1);
        em.persist(person2);

        em.clear();

        PersonMTM p1 = em.find(PersonMTM.class, "p1");
        Assert.assertEquals("Dev", p1.getPersonName());
        Assert.assertEquals(2, p1.getAddresses().size());

        PersonMTM p2 = em.find(PersonMTM.class, "p2");
        Assert.assertEquals("PG", p2.getPersonName());
        Assert.assertEquals(2, p2.getAddresses().size());

        AddressMTM a1 = em.find(AddressMTM.class, "a1");
        Assert.assertEquals("AAAAAAAAAAAAA", a1.getStreet());

        AddressMTM a2 = em.find(AddressMTM.class, "a2");
        Assert.assertEquals("BBBBBBBBBBBBBBB", a2.getStreet());

        AddressMTM a3 = em.find(AddressMTM.class, "a3");
        Assert.assertEquals("CCCCCCCCCCC", a3.getStreet());
    }

    /**
     * Test update.
     */
    private void testUpdate()
    {
        PersonMTM p1 = em.find(PersonMTM.class, "p1");
        p1.setPersonName("Devender");
        em.merge(p1);

        em.clear();

        PersonMTM p = em.find(PersonMTM.class, "p1");
        Assert.assertEquals("Devender", p.getPersonName());
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        PersonMTM p1 = em.find(PersonMTM.class, "p1");
        Assert.assertNotNull(p1);

        em.remove(p1);

        PersonMTM p = em.find(PersonMTM.class, "p1");
        Assert.assertNull(p);
    }

}
