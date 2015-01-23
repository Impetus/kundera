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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.entities.AddressMTORedis;
import com.impetus.client.entities.PersonMTORedis;

public class RedisAssociationTestMTO
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

        AddressMTORedis address = new AddressMTORedis(100.0);
        address.setAddress("Gr. Noida");

        PersonMTORedis person1 = getPerson("1", "User1", 20, address);
        PersonMTORedis person2 = getPerson("2", "User2", 40, address);
        em.persist(person1);
        em.persist(person2);

        PersonMTORedis resultPerson1 = em.find(PersonMTORedis.class, "1");
        PersonMTORedis resultPerson2 = em.find(PersonMTORedis.class, "2");

        notNullCheck(resultPerson1, resultPerson2);
        Assert.assertEquals("1", resultPerson1.getPersonId());
        Assert.assertEquals("User1", resultPerson1.getPersonName());
        Assert.assertEquals(20, resultPerson1.getAge().intValue());

        Assert.assertEquals("2", resultPerson2.getPersonId());
        Assert.assertEquals("User2", resultPerson2.getPersonName());
        Assert.assertEquals(40, resultPerson2.getAge().intValue());

        Assert.assertEquals(100.0, resultPerson1.getAddress().getAddressId());
        Assert.assertEquals("Gr. Noida", resultPerson1.getAddress().getAddress());
        Assert.assertEquals(100.0, resultPerson2.getAddress().getAddressId());
        Assert.assertEquals("Gr. Noida", resultPerson2.getAddress().getAddress());

        em.clear();
        address.setAddress("NCR");
        person1.setPersonName("User3");
        person1.setAddress(address);

        em.merge(person1);
        em = getNewEM();

        resultPerson1 = em.find(PersonMTORedis.class, "1");
        resultPerson2 = em.find(PersonMTORedis.class, "2");

        notNullCheck(resultPerson1, resultPerson2);
        Assert.assertEquals("1", resultPerson1.getPersonId());
        Assert.assertEquals("User3", resultPerson1.getPersonName());
        Assert.assertEquals(20, resultPerson1.getAge().intValue());

        Assert.assertEquals(100.0, resultPerson2.getAddress().getAddressId());
        Assert.assertEquals("NCR", resultPerson2.getAddress().getAddress());

        em.remove(resultPerson1);
        em.remove(resultPerson2);

        Assert.assertNull(em.find(PersonMTORedis.class, "1"));
        Assert.assertNull(em.find(PersonMTORedis.class, "2"));
        // Assert.assertNull(em.find(AddressMTORedis.class, 100.0));
    }

    @Test
    public void testQuery()
    {

        AddressMTORedis address = new AddressMTORedis(100.0);
        address.setAddress("Gr. Noida");

        PersonMTORedis person1 = getPerson("1", "User1", 20, address);
        PersonMTORedis person2 = getPerson("2", "User2", 40, address);
        em.persist(person1);
        em.persist(person2);

        Query query = em.createQuery("Select e from PersonMTORedis e");
        List<PersonMTORedis> queryResult = query.getResultList();

        Assert.assertEquals(2, queryResult.size());
        PersonMTORedis resultPerson1 = queryResult.get(0);
        PersonMTORedis resultPerson2 = queryResult.get(1);

        notNullCheck(resultPerson1, resultPerson2);
        for (PersonMTORedis resultPerson : queryResult)
        {

            if (resultPerson.getPersonId().equals("1"))
            {
                Assert.assertEquals("User1", resultPerson.getPersonName());
                Assert.assertEquals(20, resultPerson.getAge().intValue());
            }
            else
            {
                Assert.assertEquals("2", resultPerson.getPersonId());
                Assert.assertEquals("User2", resultPerson.getPersonName());
                Assert.assertEquals(40, resultPerson.getAge().intValue());
            }
        }

        Assert.assertEquals(100.0, resultPerson1.getAddress().getAddressId());
        Assert.assertEquals("Gr. Noida", resultPerson1.getAddress().getAddress());
        Assert.assertEquals(100.0, resultPerson2.getAddress().getAddressId());
        Assert.assertEquals("Gr. Noida", resultPerson2.getAddress().getAddress());

        em.clear();
        address.setAddress("NCR");
        person1.setPersonName("User3");
        person1.setAddress(address);

        em.merge(person1);
        em = getNewEM();

        query = em.createQuery("Select e from PersonMTORedis e");
        queryResult = query.getResultList();

        Assert.assertEquals(2, queryResult.size());
        resultPerson1 = queryResult.get(0);
        resultPerson2 = queryResult.get(1);

        notNullCheck(resultPerson1, resultPerson2);
        for (PersonMTORedis resultPerson : queryResult)
        {

            if (resultPerson.getPersonId().equals("1"))
            {
                Assert.assertEquals("User3", resultPerson.getPersonName());
                Assert.assertEquals(20, resultPerson.getAge().intValue());
            }
            else
            {
                Assert.assertEquals("2", resultPerson.getPersonId());
                Assert.assertEquals("User2", resultPerson.getPersonName());
                Assert.assertEquals(40, resultPerson.getAge().intValue());
            }
        }

        Assert.assertEquals(100.0, resultPerson2.getAddress().getAddressId());
        Assert.assertEquals("NCR", resultPerson2.getAddress().getAddress());

        em.remove(resultPerson1);
        em.remove(resultPerson2);

        Assert.assertNull(em.find(PersonMTORedis.class, "1"));
        Assert.assertNull(em.find(PersonMTORedis.class, "2"));
        // Assert.assertNull(em.find(AddressMTORedis.class, 100.0));
    }

    private boolean notNullCheck(PersonMTORedis resultPerson1, PersonMTORedis resultPerson2)
    {

        Assert.assertNotNull(resultPerson1);
        Assert.assertNotNull(resultPerson2);
        Assert.assertNotNull(resultPerson1.getAddress());
        Assert.assertNotNull(resultPerson2.getAddress());

        return true;
    }

    private PersonMTORedis getPerson(String personId, String personName, int age, AddressMTORedis address)
    {

        PersonMTORedis person = new PersonMTORedis(personId);
        person.setAge(age);
        person.setPersonName(personName);
        person.setAddress(address);

        return person;
    }

    private EntityManager getNewEM()
    {
        if (em != null && em.isOpen())
        {
            em.close();
        }
        // Delete by query.
        Query query = em.createQuery("Delete from PersonMTORedis p");
        int updateCount = query.executeUpdate();
        return em = emf.createEntityManager();
    }
}
