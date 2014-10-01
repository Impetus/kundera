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

import com.impetus.client.oraclenosql.entities.AddressOracleNoSqlMTM;
import com.impetus.client.oraclenosql.entities.PersonOracleNoSqlMTM;


/**
 * @author vivek.mishra
 * 
 */
public class OracleNoSqlMTMTest {
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
        AddressOracleNoSqlMTM address1 = new AddressOracleNoSqlMTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressOracleNoSqlMTM address2 = new AddressOracleNoSqlMTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        AddressOracleNoSqlMTM address3 = new AddressOracleNoSqlMTM();
        address3.setAddressId("c");
        address3.setStreet("sector 13");

        Set<AddressOracleNoSqlMTM> addresses1 = new HashSet<AddressOracleNoSqlMTM>();
        addresses1.add(address1);
        addresses1.add(address2);

        Set<AddressOracleNoSqlMTM> addresses2 = new HashSet<AddressOracleNoSqlMTM>();
        addresses2.add(address2);
        addresses2.add(address3);

        PersonOracleNoSqlMTM person1 = new PersonOracleNoSqlMTM();
        person1.setPersonId("1");
        person1.setPersonName("Kuldeep");

        PersonOracleNoSqlMTM person2 = new PersonOracleNoSqlMTM();
        person2.setPersonId("2");
        person2.setPersonName("vivek");

        person1.setAddresses(addresses1);
        person2.setAddresses(addresses2);

        em.persist(person1);
        em.persist(person2);

        em = getNewEM();

        PersonOracleNoSqlMTM foundPerson1 = em.find(PersonOracleNoSqlMTM.class, "1");
        Assert.assertNotNull(foundPerson1);
        Assert.assertNotNull(foundPerson1.getAddresses());
        Assert.assertEquals("1", foundPerson1.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson1.getPersonName());

        int counter = 0;
        for (AddressOracleNoSqlMTM address : foundPerson1.getAddresses()) {
            if (address.getAddressId().equals("a")) {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            } else {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        PersonOracleNoSqlMTM foundPerson2 = em.find(PersonOracleNoSqlMTM.class, "2");
        Assert.assertNotNull(foundPerson2);
        Assert.assertNotNull(foundPerson2.getAddresses());
        Assert.assertEquals("2", foundPerson2.getPersonId());
        Assert.assertEquals("vivek", foundPerson2.getPersonName());

        counter = 0;
        for (AddressOracleNoSqlMTM address : foundPerson2.getAddresses()) {
            if (address.getAddressId().equals("b")) {
                counter++;
                Assert.assertEquals("sector 12", address.getStreet());
            } else {
                Assert.assertEquals("c", address.getAddressId());
                Assert.assertEquals("sector 13", address.getStreet());
                counter++;
            }
        }

        foundPerson1.setPersonName("KK");

        foundPerson2.setPersonName("vives");

        em.merge(foundPerson1);
        em.merge(foundPerson2);

        em = getNewEM();

        foundPerson1 = em.find(PersonOracleNoSqlMTM.class, "1");
        Assert.assertNotNull(foundPerson1);
        Assert.assertNotNull(foundPerson1.getAddresses());
        Assert.assertEquals("1", foundPerson1.getPersonId());
        Assert.assertEquals("KK", foundPerson1.getPersonName());

        counter = 0;
        for (AddressOracleNoSqlMTM address : foundPerson1.getAddresses()) {
            if (address.getAddressId().equals("a")) {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            } else {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        foundPerson2 = em.find(PersonOracleNoSqlMTM.class, "2");
        Assert.assertNotNull(foundPerson2);
        Assert.assertNotNull(foundPerson2.getAddresses());
        Assert.assertEquals("2", foundPerson2.getPersonId());
        Assert.assertEquals("vives", foundPerson2.getPersonName());

        counter = 0;
        for (AddressOracleNoSqlMTM address : foundPerson2.getAddresses()) {
            if (address.getAddressId().equals("b")) {
                counter++;
                Assert.assertEquals("sector 12", address.getStreet());
            } else {
                Assert.assertEquals("c", address.getAddressId());
                Assert.assertEquals("sector 13", address.getStreet());
                counter++;
            }
        }

        em.remove(foundPerson1);
        em.remove(foundPerson2);

        foundPerson1 = em.find(PersonOracleNoSqlMTM.class, "1");
        foundPerson2 = em.find(PersonOracleNoSqlMTM.class, "2");

        Assert.assertNull(foundPerson1);
        Assert.assertNull(foundPerson2);
    }

    @Test
    public void testQuery() {
        AddressOracleNoSqlMTM address1 = new AddressOracleNoSqlMTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressOracleNoSqlMTM address2 = new AddressOracleNoSqlMTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        AddressOracleNoSqlMTM address3 = new AddressOracleNoSqlMTM();
        address3.setAddressId("c");
        address3.setStreet("sector 13");

        Set<AddressOracleNoSqlMTM> addresses1 = new HashSet<AddressOracleNoSqlMTM>();
        addresses1.add(address1);
        addresses1.add(address2);

        Set<AddressOracleNoSqlMTM> addresses2 = new HashSet<AddressOracleNoSqlMTM>();
        addresses2.add(address2);
        addresses2.add(address3);

        PersonOracleNoSqlMTM person1 = new PersonOracleNoSqlMTM();
        person1.setPersonId("1");
        person1.setPersonName("Kuldeep");

        PersonOracleNoSqlMTM person2 = new PersonOracleNoSqlMTM();
        person2.setPersonId("2");
        person2.setPersonName("vivek");

        person1.setAddresses(addresses1);
        person2.setAddresses(addresses2);

        em.persist(person1);
        em.persist(person2);

        em = getNewEM();

        String personsQueryStr = " Select p from PersonOracleNoSqlMTM p";
        Query personsQuery = em.createQuery(personsQueryStr);
        List<PersonOracleNoSqlMTM> allPersons = personsQuery.getResultList();
        Assert.assertNotNull(allPersons);

        for (PersonOracleNoSqlMTM foundPerson : allPersons) {
            Assert.assertNotNull(foundPerson);
            Assert.assertNotNull(foundPerson.getAddresses());
            Assert.assertEquals(2, foundPerson.getAddresses().size());
            
            if (foundPerson.getPersonId().equals("1")) {

                
                Assert.assertEquals("1", foundPerson.getPersonId());
                Assert.assertEquals("Kuldeep", foundPerson.getPersonName());

                for (AddressOracleNoSqlMTM address : foundPerson.getAddresses()) {
                    if (address.getAddressId().equals("a")) {

                        Assert.assertEquals("sector 11", address.getStreet());
                    } else {
                        Assert.assertEquals("b", address.getAddressId());
                        Assert.assertEquals("sector 12", address.getStreet());

                    }
                }
            } else if (foundPerson.getPersonId().equals("2")) {

  
                Assert.assertEquals("2", foundPerson.getPersonId());
                Assert.assertEquals("vivek", foundPerson.getPersonName());

                for (AddressOracleNoSqlMTM address : foundPerson.getAddresses()) {
                    if (address.getAddressId().equals("b")) {

                        Assert.assertEquals("sector 12", address.getStreet());
                    } else {
                        Assert.assertEquals("c", address.getAddressId());
                        Assert.assertEquals("sector 13", address.getStreet());

                    }
                }
            }
          
        }
        em.remove(allPersons.get(0));
        em.remove(allPersons.get(1));
        PersonOracleNoSqlMTM  foundPerson1 = em.find(PersonOracleNoSqlMTM.class, allPersons.get(0).getPersonId());
        PersonOracleNoSqlMTM  foundPerson2 = em.find(PersonOracleNoSqlMTM.class, allPersons.get(1).getPersonId());
        Assert.assertNull(foundPerson1);
        Assert.assertNull(foundPerson2);

    }

    private EntityManager getNewEM() {
        if (em != null && em.isOpen()) {
            em.close();
        }
        return em = emf.createEntityManager();
    }

}
