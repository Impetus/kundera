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
package com.impetus.client.couchdb.crud;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.couchdb.entities.AddressCouchOTM;
import com.impetus.client.couchdb.entities.PersonCouchOTM;

/**
 * @author impadmin
 * 
 */
public class CouchOTMTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("couchdb_pu");
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
        AddressCouchOTM address1 = new AddressCouchOTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressCouchOTM address2 = new AddressCouchOTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        Set<AddressCouchOTM> addresses = new HashSet<AddressCouchOTM>();
        addresses.add(address1);
        addresses.add(address2);

        PersonCouchOTM person = new PersonCouchOTM();
        person.setPersonId("1");
        person.setPersonName("Kuldeep");
        person.setAddresses(addresses);

        em.persist(person);

        em = getNewEM();

        PersonCouchOTM foundPerson = em.find(PersonCouchOTM.class, 1);
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddresses());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson.getPersonName());

        int counter = 0;
        for (AddressCouchOTM address : foundPerson.getAddresses())
        {
            if (address.getAddressId().equals("a"))
            {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            }
            else
            {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        Assert.assertEquals(2, counter);

        foundPerson.setPersonName("KK");

        em.merge(foundPerson);

        em = getNewEM();

        foundPerson = em.find(PersonCouchOTM.class, 1);
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddresses());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("KK", foundPerson.getPersonName());

        counter = 0;
        for (AddressCouchOTM address : foundPerson.getAddresses())
        {
            if (address.getAddressId().equals("a"))
            {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            }
            else
            {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        Assert.assertEquals(2, counter);

        em.remove(foundPerson);

        foundPerson = em.find(PersonCouchOTM.class, 1);
        Assert.assertNull(foundPerson);
    }

    @Test
    public void testQuery()
    {
        AddressCouchOTM address1 = new AddressCouchOTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressCouchOTM address2 = new AddressCouchOTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        Set<AddressCouchOTM> addresses = new HashSet<AddressCouchOTM>();
        addresses.add(address1);
        addresses.add(address2);

        PersonCouchOTM person = new PersonCouchOTM();
        person.setPersonId("1");
        person.setPersonName("Kuldeep");
        person.setAddresses(addresses);

        em.persist(person);

        PersonCouchOTM person1 = new PersonCouchOTM();
        person1.setPersonId("2");
        person1.setPersonName("KK");

        address1.setAddressId("a1");
        address1.setStreet("Sector a1");

        address2.setAddressId("a2");
        address2.setStreet("Sector a2");

        AddressCouchOTM address3 = new AddressCouchOTM();
        address3.setAddressId("a3");
        address3.setStreet("Sector a3");

        addresses = new HashSet<AddressCouchOTM>();
        addresses.add(address1);
        addresses.add(address2);
        addresses.add(address3);

        person1.setAddresses(addresses);

        em.persist(person1);

        em = getNewEM();

        Query q = em.createQuery("Select p from PersonCouchOTM p");
        List<PersonCouchOTM> results = q.getResultList();
        Assert.assertEquals(2, results.size());

        for (PersonCouchOTM foundPerson : results)
        {
            if (foundPerson.getPersonId().equals("1"))
            {

                Assert.assertEquals("Kuldeep", foundPerson.getPersonName());
                Assert.assertEquals(2, foundPerson.getAddresses().size());

                for (AddressCouchOTM address : foundPerson.getAddresses())
                {
                    if (address.getAddressId().equals("a"))
                    {
                        Assert.assertEquals("sector 11", address.getStreet());
                    }
                    else
                    {
                        Assert.assertEquals("b", address.getAddressId());
                        Assert.assertEquals("sector 12", address.getStreet());

                    }
                }

            }
            else if (foundPerson.getPersonId().equals("2"))
            {

                Assert.assertEquals("KK", foundPerson.getPersonName());
                Assert.assertEquals(3, foundPerson.getAddresses().size());

                for (AddressCouchOTM address : foundPerson.getAddresses())
                {
                    if (address.getAddressId().equals("a1"))
                    {

                        Assert.assertEquals("Sector a1", address.getStreet());

                    }
                    else if (address.getAddressId().equals("a2"))
                    {

                        Assert.assertEquals("Sector a2", address.getStreet());

                    }
                    else
                    {

                        Assert.assertEquals("a3", address.getAddressId());
                        Assert.assertEquals("Sector a3", address.getStreet());

                    }
                }

            }
        }
        em.remove(person);
        em.remove(person1);

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
