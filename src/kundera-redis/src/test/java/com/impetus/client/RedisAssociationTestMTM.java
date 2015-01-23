/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.entities.AddressMTMRedis;
import com.impetus.client.entities.PersonMTMRedis;

public class RedisAssociationTestMTM
{

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("redis_pu");
        em = getNewEM();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }

    @Test
    public void testCrud()
    {

        AddressMTMRedis address1 = getAddress(101.0, "NCR");
        AddressMTMRedis address2 = getAddress(109.0, "Delhi");
        AddressMTMRedis address3 = getAddress(145.0, "Gurgaon");

        Set<AddressMTMRedis> addresses1 = new HashSet<AddressMTMRedis>();
        addresses1.add(address1);
        addresses1.add(address2);

        Set<AddressMTMRedis> addresses2 = new HashSet<AddressMTMRedis>();
        addresses2.add(address2);
        addresses2.add(address3);

        PersonMTMRedis person1 = getPerson("1", "User1", 20, addresses1);
        PersonMTMRedis person2 = getPerson("2", "User2", 40, addresses2);

        em.persist(person1);
        em.persist(person2);

        PersonMTMRedis resultPerson1 = em.find(PersonMTMRedis.class, "1");
        PersonMTMRedis resultPerson2 = em.find(PersonMTMRedis.class, "2");

        notNullCheck(resultPerson1, resultPerson2);
        Assert.assertEquals("1", resultPerson1.getPersonId());
        Assert.assertEquals("User1", resultPerson1.getPersonName());
        Assert.assertEquals(20, resultPerson1.getAge().intValue());

        Assert.assertEquals("2", resultPerson2.getPersonId());
        Assert.assertEquals("User2", resultPerson2.getPersonName());
        Assert.assertEquals(40, resultPerson2.getAge().intValue());

        Assert.assertEquals(2, resultPerson1.getAddress().size());
        Assert.assertEquals(2, resultPerson2.getAddress().size());

        for (AddressMTMRedis address : resultPerson1.getAddress())
        {
            if (address.getAddressId().equals(101.0))
            {
                Assert.assertEquals("NCR", address.getAddress());
            }
            else
            {
                Assert.assertEquals(109.0, address.getAddressId());
                Assert.assertEquals("Delhi", address.getAddress());
            }
        }

        for (AddressMTMRedis address : resultPerson2.getAddress())
        {
            if (address.getAddressId().equals(109.0))
            {
                Assert.assertEquals("Delhi", address.getAddress());
            }
            else
            {
                Assert.assertEquals(145.0, address.getAddressId());
                Assert.assertEquals("Gurgaon", address.getAddress());
            }
        }

        em.clear();
        address1.setAddress("Agra");
        address2.setAddress("Mathura");
        person1.setPersonName("User3");

        em.merge(person1);
        em = getNewEM();

        resultPerson1 = em.find(PersonMTMRedis.class, "1");
        resultPerson2 = em.find(PersonMTMRedis.class, "2");

        notNullCheck(resultPerson1, resultPerson2);
        Assert.assertEquals("1", resultPerson1.getPersonId());
        Assert.assertEquals("User3", resultPerson1.getPersonName());
        Assert.assertEquals(20, resultPerson1.getAge().intValue());

        for (AddressMTMRedis address : resultPerson1.getAddress())
        {
            if (address.getAddressId().equals(101.0))
            {
                Assert.assertEquals("Agra", address.getAddress());
            }
            else
            {
                Assert.assertEquals(109.0, address.getAddressId());
                Assert.assertEquals("Mathura", address.getAddress());
            }
        }

        for (AddressMTMRedis address : resultPerson2.getAddress())
        {
            if (address.getAddressId().equals(109.0))
            {
                Assert.assertEquals("Mathura", address.getAddress());
            }
            else
            {
                Assert.assertEquals(145.0, address.getAddressId());
                Assert.assertEquals("Gurgaon", address.getAddress());
            }
        }

        em.remove(resultPerson1);
        em.remove(resultPerson2);

        Assert.assertNull(em.find(PersonMTMRedis.class, "1"));
        Assert.assertNull(em.find(PersonMTMRedis.class, "2"));
        Assert.assertNull(em.find(AddressMTMRedis.class, 101.0));
        Assert.assertNull(em.find(AddressMTMRedis.class, 109.0));
        Assert.assertNull(em.find(AddressMTMRedis.class, 145.0));
    }

    @Test
    public void testQuery()
    {

        AddressMTMRedis address1 = getAddress(101.0, "NCR");
        AddressMTMRedis address2 = getAddress(109.0, "Delhi");
        AddressMTMRedis address3 = getAddress(145.0, "Gurgaon");

        Set<AddressMTMRedis> addresses1 = new HashSet<AddressMTMRedis>();
        addresses1.add(address1);
        addresses1.add(address2);

        Set<AddressMTMRedis> addresses2 = new HashSet<AddressMTMRedis>();
        addresses2.add(address2);
        addresses2.add(address3);

        PersonMTMRedis person1 = getPerson("1", "User1", 20, addresses1);
        PersonMTMRedis person2 = getPerson("2", "User2", 40, addresses2);

        em.persist(person1);
        em.persist(person2);

        Query query = em.createQuery("Select e from PersonMTMRedis e");
        List<PersonMTMRedis> queryResult = query.getResultList();

        Assert.assertEquals(2, queryResult.size());

        PersonMTMRedis resultPerson1 = queryResult.get(1);
        PersonMTMRedis resultPerson2 = queryResult.get(0);

        notNullCheck(resultPerson1, resultPerson2);
        Assert.assertEquals("1", resultPerson1.getPersonId());
        Assert.assertEquals("User1", resultPerson1.getPersonName());
        Assert.assertEquals(20, resultPerson1.getAge().intValue());

        Assert.assertEquals("2", resultPerson2.getPersonId());
        Assert.assertEquals("User2", resultPerson2.getPersonName());
        Assert.assertEquals(40, resultPerson2.getAge().intValue());

        Assert.assertEquals(2, resultPerson1.getAddress().size());
        Assert.assertEquals(2, resultPerson2.getAddress().size());

        for (AddressMTMRedis address : resultPerson1.getAddress())
        {
            if (address.getAddressId().equals(101.0))
            {
                Assert.assertEquals("NCR", address.getAddress());
            }
            else
            {
                Assert.assertEquals(109.0, address.getAddressId());
                Assert.assertEquals("Delhi", address.getAddress());
            }
        }

        for (AddressMTMRedis address : resultPerson2.getAddress())
        {
            if (address.getAddressId().equals(109.0))
            {
                Assert.assertEquals("Delhi", address.getAddress());
            }
            else
            {
                Assert.assertEquals(145.0, address.getAddressId());
                Assert.assertEquals("Gurgaon", address.getAddress());
            }
        }

        em.clear();
        address1.setAddress("Agra");
        address2.setAddress("Mathura");
        person1.setPersonName("User3");

        em.merge(person1);
        em = getNewEM();

        query = em.createQuery("Select e from PersonMTMRedis e");
        queryResult = query.getResultList();

        Assert.assertEquals(2, queryResult.size());

        if (queryResult.get(0).getPersonId().equals(1))
        {
            resultPerson1 = queryResult.get(0);
            resultPerson2 = queryResult.get(1);
        }
        else
        {
            resultPerson1 = queryResult.get(1);
            resultPerson2 = queryResult.get(0);
        }

        notNullCheck(resultPerson1, resultPerson2);

        Assert.assertEquals("1", resultPerson1.getPersonId());
        Assert.assertEquals("User3", resultPerson1.getPersonName());
        Assert.assertEquals(20, resultPerson1.getAge().intValue());

        for (AddressMTMRedis address : resultPerson1.getAddress())
        {
            if (address.getAddressId().equals(101.0))
            {
                Assert.assertEquals("Agra", address.getAddress());
            }
            else
            {
                Assert.assertEquals(109.0, address.getAddressId());
                Assert.assertEquals("Mathura", address.getAddress());
            }
        }

        for (AddressMTMRedis address : resultPerson2.getAddress())
        {
            if (address.getAddressId().equals(109.0))
            {
                Assert.assertEquals("Mathura", address.getAddress());
            }
            else
            {
                Assert.assertEquals(145.0, address.getAddressId());
                Assert.assertEquals("Gurgaon", address.getAddress());
            }
        }

        em.remove(resultPerson1);
        em.remove(resultPerson2);

        Assert.assertNull(em.find(PersonMTMRedis.class, "1"));
        Assert.assertNull(em.find(PersonMTMRedis.class, "2"));
        Assert.assertNull(em.find(AddressMTMRedis.class, 101.0));
        Assert.assertNull(em.find(AddressMTMRedis.class, 109.0));
        Assert.assertNull(em.find(AddressMTMRedis.class, 145.0));
    }

    private boolean notNullCheck(PersonMTMRedis resultPerson1, PersonMTMRedis resultPerson2)
    {

        Assert.assertNotNull(resultPerson1);
        Assert.assertNotNull(resultPerson2);
        Assert.assertNotNull(resultPerson1.getAddress());
        Assert.assertNotNull(resultPerson2.getAddress());

        return true;
    }

    private PersonMTMRedis getPerson(String personId, String personName, int age, Set<AddressMTMRedis> address)
    {

        PersonMTMRedis person = new PersonMTMRedis(personId);
        person.setAge(age);
        person.setPersonName(personName);
        person.setAddress(address);

        return person;
    }

    private AddressMTMRedis getAddress(Double addressId, String personAddress)
    {

        AddressMTMRedis address = new AddressMTMRedis(addressId);
        address.setAddress(personAddress);
        return address;
    }

    private EntityManager getNewEM()
    {
        if (em != null && em.isOpen())
        {
            em.close();
        }
        // Delete by query.
        Query query = em.createQuery("Delete from PersonMTMRedis p");
        int updateCount = query.executeUpdate();
        return em = emf.createEntityManager();
    }
}
