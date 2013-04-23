/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.oraclenosql;

import static org.junit.Assert.assertNotNull;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.oraclenosql.entities.AddressOTOOracleNoSQL;
import com.impetus.client.oraclenosql.entities.PersonOTOOracleNoSQL;

/**
 * Test case for association between two entities in OracleNoSQL 
 * @author amresh.singh
 */
public class OracleNoSQLAssociationTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("twikvstore");
        em = emf.createEntityManager();
    }

    @After
    public void tearDown()
    {
        em.close();
        emf.close();
    }

    @Test
    public void executeTest()
    {
        //Insert records
        persistPerson("1", "person1", 10, new AddressOTOOracleNoSQL(1.1, "Address 1"));
        persistPerson("2", "person2", 20, new AddressOTOOracleNoSQL(2.2, "Address 2"));
        persistPerson("3", "person3", 30, new AddressOTOOracleNoSQL(3.3, "Address 3"));
        persistPerson("4", "person4", 40, new AddressOTOOracleNoSQL(4.4, "Address 4"));

        //Find Records
        em.clear();
        PersonOTOOracleNoSQL p11 = findById("1");
        Assert.assertNotNull(p11);
        Assert.assertEquals("person1", p11.getPersonName());
        Assert.assertEquals(new Integer(10), p11.getAge());
        Assert.assertNotNull(p11.getAddress());
        Assert.assertEquals(new Double(1.1), p11.getAddress().getAddressId());
        Assert.assertEquals("Address 1", p11.getAddress().getStreet());        

        PersonOTOOracleNoSQL p22 = findById("2");
        Assert.assertNotNull(p22);
        Assert.assertEquals("person2", p22.getPersonName());
        Assert.assertEquals(new Integer(20), p22.getAge());
        Assert.assertNotNull(p22.getAddress());
        Assert.assertEquals(new Double(2.2), p22.getAddress().getAddressId());
        Assert.assertEquals("Address 2", p22.getAddress().getStreet()); 
        
        PersonOTOOracleNoSQL p33 = findById("3");
        Assert.assertNotNull(p33);
        Assert.assertEquals("person3", p33.getPersonName());
        Assert.assertEquals(new Integer(30), p33.getAge());
        Assert.assertNotNull(p33.getAddress());
        Assert.assertEquals(new Double(3.3), p33.getAddress().getAddressId());
        Assert.assertEquals("Address 3", p33.getAddress().getStreet()); 

        PersonOTOOracleNoSQL p44 = findById("4");
        assertNotNull(p44);
        Assert.assertEquals("person4", p44.getPersonName());
        Assert.assertEquals(new Integer(40), p44.getAge());
        Assert.assertNotNull(p44.getAddress());
        Assert.assertEquals(new Double(4.4), p44.getAddress().getAddressId());
        Assert.assertEquals("Address 4", p44.getAddress().getStreet()); 

        PersonOTOOracleNoSQL p55 = findById("5");  //Invalid records
        Assert.assertNull(p55);

        //Update records
        p11.setPersonName("person11"); p11.setAge(100); p11.getAddress().setStreet("Address 11"); updatePerson(p11);
        p22.setPersonName("person22"); p22.setAge(200); p22.getAddress().setStreet("Address 22"); updatePerson(p22);
        p33.setPersonName("person33"); p33.setAge(300); p33.getAddress().setStreet("Address 33"); updatePerson(p33);
        p44.setPersonName("person44"); p44.setAge(400); p44.getAddress().setStreet("Address 44"); updatePerson(p44);
        em.clear();        
        p11 = findById("1");
        Assert.assertNotNull(p11);
        Assert.assertEquals("person11", p11.getPersonName());
        Assert.assertEquals(new Integer(100), p11.getAge());
        Assert.assertEquals("Address 11", p11.getAddress().getStreet());

        p22 = findById("2");
        assertNotNull(p22);
        Assert.assertEquals("person22", p22.getPersonName());
        Assert.assertEquals(new Integer(200), p22.getAge());
        Assert.assertEquals("Address 22", p22.getAddress().getStreet());

        p33 = findById("3");
        assertNotNull(p33);
        Assert.assertEquals("person33", p33.getPersonName());
        Assert.assertEquals(new Integer(300), p33.getAge());
        Assert.assertEquals("Address 33", p33.getAddress().getStreet());

        p44 = findById("4");
        Assert.assertNotNull(p44);
        Assert.assertEquals("person44", p44.getPersonName());
        Assert.assertEquals(new Integer(400), p44.getAge());
        Assert.assertEquals("Address 44", p44.getAddress().getStreet());
        
        // Delete Records
        deletePerson(p11);
        deletePerson(p22);
        deletePerson(p33);
        deletePerson(p44);

        em.clear();
        Assert.assertNull(findById("1"));
        Assert.assertNull(findById("2"));
        Assert.assertNull(findById("3"));
        Assert.assertNull(findById("4"));
        
    }
    
    
    
    protected void persistPerson(String personId, String personName, int age, AddressOTOOracleNoSQL address)
    {
        Object p = preparePerson(personId, age, personName, address);
        em.persist(p);
    }    

    protected PersonOTOOracleNoSQL findById(Object personId)
    {
        return em.find(PersonOTOOracleNoSQL.class, personId);
    }

    protected void updatePerson(PersonOTOOracleNoSQL person)
    {
        em.merge(person);
    }
    
    protected void deletePerson(PersonOTOOracleNoSQL person)
    {
        em.remove(person);
    }
    
    protected PersonOTOOracleNoSQL preparePerson(String rowKey, int age, String name, AddressOTOOracleNoSQL address)
    {
        PersonOTOOracleNoSQL person = new PersonOTOOracleNoSQL();
        person.setPersonId(rowKey);
        person.setPersonName(name);
        person.setAge(age);
        person.setAddress(address);
        return person;
    }

}
