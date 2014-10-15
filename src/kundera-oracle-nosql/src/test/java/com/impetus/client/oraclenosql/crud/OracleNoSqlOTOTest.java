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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.oraclenosql.entities.AddressOracleNoSqlOTO;
import com.impetus.client.oraclenosql.entities.PersonOracleNoSqlOTO;

/**
 * @author vivek.mishra
 * 
 */
public class OracleNoSqlOTOTest {
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
        AddressOracleNoSqlOTO address = new AddressOracleNoSqlOTO();
        address.setAddressId("a");
        address.setStreet("sector 11");

        PersonOracleNoSqlOTO person = new PersonOracleNoSqlOTO();
        person.setPersonId("1");
        person.setPersonName("Kuldeep");
        person.setAddress(address);

        em.persist(person);

        em = getNewEM();

        PersonOracleNoSqlOTO foundPerson = em.find(PersonOracleNoSqlOTO.class, "1");
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddress());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson.getPersonName());
        Assert.assertEquals("a", foundPerson.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson.getAddress().getStreet());

        foundPerson.setPersonName("KK");
        foundPerson.getAddress().setStreet("sector 12");

        em.merge(foundPerson);

        em = getNewEM();

        foundPerson = em.find(PersonOracleNoSqlOTO.class, "1");
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddress());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("KK", foundPerson.getPersonName());
        Assert.assertEquals("a", foundPerson.getAddress().getAddressId());
        Assert.assertEquals("sector 12", foundPerson.getAddress().getStreet());

        em.remove(foundPerson);
        foundPerson = em.find(PersonOracleNoSqlOTO.class, "1");
        Assert.assertNull(foundPerson);

    }

    @Test
    public void testQuery() {
        AddressOracleNoSqlOTO address = new AddressOracleNoSqlOTO();
        address.setAddressId("a");
        address.setStreet("sector 11");

        AddressOracleNoSqlOTO address1 = new AddressOracleNoSqlOTO();
        address1.setAddressId("a1");
        address1.setStreet("sector 12");

        PersonOracleNoSqlOTO person = new PersonOracleNoSqlOTO();
        person.setPersonId("1");
        person.setPersonName("Kuldeep");
        person.setAddress(address);

        PersonOracleNoSqlOTO person1 = new PersonOracleNoSqlOTO();
        person1.setPersonId("2");
        person1.setPersonName("KK");
        person1.setAddress(address1);
        em.persist(person1);

        em.persist(person);

        em = getNewEM();

        em = getNewEM();

        // Query on Persons
        String personsQueryStr = " Select p from PersonOracleNoSqlOTO p";
        Query personsQuery = em.createQuery(personsQueryStr);
        List<PersonOracleNoSqlOTO> allPersons = personsQuery.getResultList();
        Assert.assertNotNull(allPersons);

        for (PersonOracleNoSqlOTO foundPerson : allPersons) {
            Assert.assertNotNull(foundPerson);
            Assert.assertNotNull(foundPerson.getAddress());
            if (foundPerson.getPersonId().equals("1")) {
                
                Assert.assertEquals("Kuldeep", foundPerson.getPersonName());
                Assert.assertEquals("a", foundPerson.getAddress().getAddressId());
                Assert.assertEquals("sector 11", foundPerson.getAddress().getStreet());
                
            } else if (foundPerson.getPersonId().equals("2")) {
                
                Assert.assertEquals("KK", foundPerson.getPersonName());
                Assert.assertEquals("a1", foundPerson.getAddress().getAddressId());
                Assert.assertEquals("sector 12", foundPerson.getAddress().getStreet());
                
            }
            em.remove(foundPerson);
            foundPerson = em.find(PersonOracleNoSqlOTO.class, foundPerson.getPersonId());
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