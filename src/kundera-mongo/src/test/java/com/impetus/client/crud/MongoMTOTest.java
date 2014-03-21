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
package com.impetus.client.crud;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.PersonMongoMTO;

public class MongoMTOTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("mongoTest");
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
    public void testCRUD()
    {
        AddressMongoMTO address = new AddressMongoMTO();
        address.setAddressId("a");
        address.setStreet("sector 11");

        PersonMongoMTO person1 = new PersonMongoMTO();
        person1.setPersonId("1");
        person1.setPersonName("Kuldeep");

        PersonMongoMTO person2 = new PersonMongoMTO();
        person2.setPersonId("2");
        person2.setPersonName("vivek");

        person1.setAddress(address);
        person2.setAddress(address);

        em.persist(person1);
        em.persist(person2);

        em = getNewEM();

        PersonMongoMTO foundPerson1 = em.find(PersonMongoMTO.class, "1");
        Assert.assertNotNull(foundPerson1);
        Assert.assertNotNull(foundPerson1.getAddress());
        Assert.assertEquals("1", foundPerson1.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson1.getPersonName());
        Assert.assertEquals("a", foundPerson1.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson1.getAddress().getStreet());

        PersonMongoMTO foundPerson2 = em.find(PersonMongoMTO.class, "2");
        Assert.assertNotNull(foundPerson2);
        Assert.assertNotNull(foundPerson2.getAddress());
        Assert.assertEquals("2", foundPerson2.getPersonId());
        Assert.assertEquals("vivek", foundPerson2.getPersonName());
        Assert.assertEquals("a", foundPerson2.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson2.getAddress().getStreet());

        foundPerson1.setPersonName("KK");

        foundPerson2.setPersonName("vives");

        em.merge(foundPerson1);
        em.merge(foundPerson2);

        em = getNewEM();

        foundPerson1 = em.find(PersonMongoMTO.class, "1");
        Assert.assertNotNull(foundPerson1);
        Assert.assertNotNull(foundPerson1.getAddress());
        Assert.assertEquals("1", foundPerson1.getPersonId());
        Assert.assertEquals("KK", foundPerson1.getPersonName());
        Assert.assertEquals("a", foundPerson1.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson1.getAddress().getStreet());

        foundPerson2 = em.find(PersonMongoMTO.class, "2");
        Assert.assertNotNull(foundPerson2);
        Assert.assertNotNull(foundPerson2.getAddress());
        Assert.assertEquals("2", foundPerson2.getPersonId());
        Assert.assertEquals("vives", foundPerson2.getPersonName());
        Assert.assertEquals("a", foundPerson2.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson2.getAddress().getStreet());

        em.remove(foundPerson1);
        em.remove(foundPerson2);

        foundPerson1 = em.find(PersonMongoMTO.class, "1");
        foundPerson2 = em.find(PersonMongoMTO.class, "2");

        Assert.assertNull(foundPerson1);
        Assert.assertNull(foundPerson2);
    }

    private EntityManager getNewEM()
    {
        if (em != null && em.isOpen())
        {
            em.close();
        }
        return em = emf.createEntityManager();
    }

}
