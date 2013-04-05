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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.oraclenosql.entities.Office;
import com.impetus.client.oraclenosql.entities.PersonEmbeddedKVStore;

/**
 * Test case for CRUD and query operations on an entity that contains one embeddable attribute 
 * @author amresh.singh
 */
public class OracleNoSQLEmbeddableTest
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

    /**
     * On insert cassandra.
     */
    @Test
    public void executeTest()
    {
        
        //Insert records
        persistPerson("1", "person1", 10, new Office(1, "Company 1", "Location 1"));
        persistPerson("2", "person2", 20, new Office(2, "Company 2", "Location 2"));
        persistPerson("3", "person3", 30, new Office(3, "Company 3", "Location 3"));
        persistPerson("4", "person4", 40, new Office(4, "Company 4", "Location 4"));

        //Find Records
        em.clear();
        PersonEmbeddedKVStore p11 = findById("1");
        assertNotNull(p11);
        assertEquals("person1", p11.getPersonName());
        assertEquals(10, p11.getAge());
        Assert.assertNotNull(p11.getOffice());
        Assert.assertEquals(1, p11.getOffice().getOfficeId());
        Assert.assertEquals("Company 1", p11.getOffice().getCompanyName());
        Assert.assertEquals("Location 1", p11.getOffice().getLocation());

        PersonEmbeddedKVStore p22 = findById("2");
        assertNotNull(p22);
        assertEquals("person2", p22.getPersonName());
        assertEquals(20, p22.getAge());
        Assert.assertNotNull(p22.getOffice());
        Assert.assertEquals(2, p22.getOffice().getOfficeId());
        Assert.assertEquals("Company 2", p22.getOffice().getCompanyName());
        Assert.assertEquals("Location 2", p22.getOffice().getLocation());
        
        PersonEmbeddedKVStore p33 = findById("3");
        assertNotNull(p33);
        assertEquals("person3", p33.getPersonName());
        assertEquals(30, p33.getAge());
        Assert.assertNotNull(p33.getOffice());
        Assert.assertEquals(3, p33.getOffice().getOfficeId());
        Assert.assertEquals("Company 3", p33.getOffice().getCompanyName());
        Assert.assertEquals("Location 3", p33.getOffice().getLocation());

        PersonEmbeddedKVStore p44 = findById("4");
        assertNotNull(p44);
        assertEquals("person4", p44.getPersonName());
        assertEquals(40, p44.getAge());
        Assert.assertNotNull(p44.getOffice());
        Assert.assertEquals(4, p44.getOffice().getOfficeId());
        Assert.assertEquals("Company 4", p44.getOffice().getCompanyName());
        Assert.assertEquals("Location 4", p44.getOffice().getLocation());

        PersonEmbeddedKVStore p55 = findById("5");  //Invalid records
        Assert.assertNull(p55);

        //Update records
        p11.setPersonName("person11"); p11.setAge(100); p11.getOffice().setCompanyName("Company 11"); updatePerson(p11);
        p22.setPersonName("person22"); p22.setAge(200); p22.getOffice().setCompanyName("Company 22"); updatePerson(p22);
        p33.setPersonName("person33"); p33.setAge(300); p33.getOffice().setCompanyName("Company 33"); updatePerson(p33);
        p44.setPersonName("person44"); p44.setAge(400); p44.getOffice().setCompanyName("Company 44"); updatePerson(p44);
        em.clear();        
        p11 = findById("1");
        assertNotNull(p11);
        assertEquals("person11", p11.getPersonName());
        assertEquals(100, p11.getAge());
        Assert.assertEquals("Company 11", p11.getOffice().getCompanyName());

        p22 = findById("2");
        assertNotNull(p22);
        assertEquals("person22", p22.getPersonName());
        assertEquals(200, p22.getAge());
        Assert.assertEquals("Company 22", p22.getOffice().getCompanyName());

        p33 = findById("3");
        assertNotNull(p33);
        assertEquals("person33", p33.getPersonName());
        assertEquals(300, p33.getAge());
        Assert.assertEquals("Company 33", p33.getOffice().getCompanyName());

        p44 = findById("4");
        assertNotNull(p44);
        assertEquals("person44", p44.getPersonName());
        assertEquals(400, p44.getAge());
        Assert.assertEquals("Company 44", p44.getOffice().getCompanyName());
        
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
    
    protected void persistPerson(String personId, String personName, int age, Office office)
    {
        Object p = preparePerson(personId, age, personName, office);
        em.persist(p);
    }

    protected PersonEmbeddedKVStore preparePerson(String rowKey, int age, String name, Office office)
    {
        PersonEmbeddedKVStore person = new PersonEmbeddedKVStore();
        person.setPersonId(rowKey);
        person.setPersonName(name);
        person.setAge(age);
        person.setOffice(office);        
        return person;
    }

    protected PersonEmbeddedKVStore findById(Object personId)
    {
        return em.find(PersonEmbeddedKVStore.class, personId);
    }

    protected void updatePerson(PersonEmbeddedKVStore person)
    {
        em.merge(person);
    }
    
    protected void deletePerson(PersonEmbeddedKVStore person)
    {
        em.remove(person);
    }

}
