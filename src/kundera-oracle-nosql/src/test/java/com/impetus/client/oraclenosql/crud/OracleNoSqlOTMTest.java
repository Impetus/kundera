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
package com.impetus.client.oraclenosql.crud;

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

import com.impetus.client.oraclenosql.entities.AddressOracleNoSqlOTM;
import com.impetus.client.oraclenosql.entities.PersonOracleNoSqlOTM;

/**
 * @author vivek.mishra
 * 
 */
public class OracleNoSqlOTMTest {
    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        emf = Persistence.createEntityManagerFactory("twikvstore");
        em = getNewEM();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        em.close();
        emf.close();
    }

    @Test
    public void testCRUD() {
        AddressOracleNoSqlOTM address1 = new AddressOracleNoSqlOTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressOracleNoSqlOTM address2 = new AddressOracleNoSqlOTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        Set<AddressOracleNoSqlOTM> addresses = new HashSet<AddressOracleNoSqlOTM>();
        addresses.add(address1);
        addresses.add(address2);

        PersonOracleNoSqlOTM person = new PersonOracleNoSqlOTM();
        person.setPersonId("1");
        person.setPersonName("Kuldeep");
        person.setAddresses(addresses);

        em.persist(person);

        em = getNewEM();

        PersonOracleNoSqlOTM foundPerson = em.find(PersonOracleNoSqlOTM.class, "1");
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddresses());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson.getPersonName());

        int counter = 0;
        for (AddressOracleNoSqlOTM address : foundPerson.getAddresses()) {
            if (address.getAddressId().equals("a")) {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            } else {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        Assert.assertEquals(2, counter);

        foundPerson.setPersonName("KK");

        em.merge(foundPerson);

        em = getNewEM();

        foundPerson = em.find(PersonOracleNoSqlOTM.class, "1");
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddresses());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("KK", foundPerson.getPersonName());

        counter = 0;
        for (AddressOracleNoSqlOTM address : foundPerson.getAddresses()) {
            if (address.getAddressId().equals("a")) {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            } else {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        Assert.assertEquals(2, counter);

        em.remove(foundPerson);

        foundPerson = em.find(PersonOracleNoSqlOTM.class, "1");
        Assert.assertNull(foundPerson);
    }

    @Test
    public void testQuery() {
        AddressOracleNoSqlOTM address1 = new AddressOracleNoSqlOTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressOracleNoSqlOTM address2 = new AddressOracleNoSqlOTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        Set<AddressOracleNoSqlOTM> addresses = new HashSet<AddressOracleNoSqlOTM>();
        addresses.add(address1);
        addresses.add(address2);

        PersonOracleNoSqlOTM person = new PersonOracleNoSqlOTM();
        person.setPersonId("1");
        person.setPersonName("Kuldeep");
        person.setAddresses(addresses);

        em.persist(person);

        AddressOracleNoSqlOTM address1a = new AddressOracleNoSqlOTM();
        address1a.setAddressId("a1");
        address1a.setStreet("sector 11a");

        AddressOracleNoSqlOTM address2b = new AddressOracleNoSqlOTM();
        address2b.setAddressId("b1");
        address2b.setStreet("sector 12b");

        Set<AddressOracleNoSqlOTM> addresses1 = new HashSet<AddressOracleNoSqlOTM>();
        addresses1.add(address1a);
        addresses1.add(address2b);

        PersonOracleNoSqlOTM person1 = new PersonOracleNoSqlOTM();
        person1.setPersonId("2");
        person1.setPersonName("KK");
        person1.setAddresses(addresses1);

        em.persist(person1);

        em = getNewEM();

        String personsQueryStr = " Select p from PersonOracleNoSqlOTM p";
        Query personsQuery = em.createQuery(personsQueryStr);
        List<PersonOracleNoSqlOTM> allPersons = personsQuery.getResultList();
        Assert.assertNotNull(allPersons);

        Assert.assertEquals(2, allPersons.size());

        for (PersonOracleNoSqlOTM foundPerson : allPersons) {

            Assert.assertNotNull(foundPerson);
            Assert.assertEquals(2, foundPerson.getAddresses().size());
            Assert.assertNotNull(foundPerson.getAddresses());
            if (foundPerson.getPersonId().equals("1")) {

                Assert.assertEquals("Kuldeep", foundPerson.getPersonName());

                for (AddressOracleNoSqlOTM address : foundPerson.getAddresses()) {
                    if (address.getAddressId().equals("a")) {

                        Assert.assertEquals("sector 11", address.getStreet());
                    } else {
                        Assert.assertEquals("b", address.getAddressId());
                        Assert.assertEquals("sector 12", address.getStreet());

                    }
                }
            } else if (foundPerson.getPersonId().equals("2")) {
                Assert.assertEquals("KK", foundPerson.getPersonName());

                for (AddressOracleNoSqlOTM address : foundPerson.getAddresses()) {
                    if (address.getAddressId().equals("a1")) {

                        Assert.assertEquals("sector 11a", address.getStreet());
                    } else {
                        Assert.assertEquals("b1", address.getAddressId());
                        Assert.assertEquals("sector 12b", address.getStreet());

                    }
                }
            }
            em.remove(foundPerson);

            foundPerson = em.find(PersonOracleNoSqlOTM.class, foundPerson.getPersonId());
            Assert.assertNull(foundPerson);
        }

    }

    private EntityManager getNewEM() {
        if (em != null && em.isOpen()) {
            em.close();
        }
        return em = emf.createEntityManager();
    }

}
