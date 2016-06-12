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
package com.impetus.client.es.association;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author vivek.mishra junit for Elastic search association.
 */
public class ESAssociationTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;


    private static Node node = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        Builder builder = Settings.settingsBuilder();
        builder.put("path.home", "target/data");
        node = new NodeBuilder().settings(builder).node();
    }

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory("es-pu");
        em = emf.createEntityManager();
    }

    @Test
    public void testCRUD1M() throws InterruptedException
    {
        Person1M person = new Person1M();
        person.setPersonId("person_1");
        person.setPersonName("vivek");

        Address1M address1 = new Address1M();
        address1.setAddressId("noida street");
        address1.setPerson(person);
        address1.setStreet("Sector 50");

        Address1M address2 = new Address1M();
        address2.setAddressId("expressway");
        address2.setPerson(person);
        address2.setStreet("Sector 137");

        Set<Address1M> addresses = new HashSet<Address1M>();
        addresses.add(address1);
        addresses.add(address2);
        person.setAddresses(addresses);

        em.persist(person);
        waitThread();

        em.clear();

        person = em.find(Person1M.class, "person_1");
        Assert.assertNotNull(person);
        Assert.assertNotNull(person.getAddresses());
        Assert.assertEquals(2, person.getAddresses().size());

        Assert.assertEquals(person.getPersonId(), person.getAddresses().iterator().next().getPerson().getPersonId());

        Query q = em.createQuery("select p from Person1M p where p.personName = vivek");
        List<Person1M> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        Assert.assertNotNull(persons.get(0).getAddresses());
        Assert.assertEquals(2, persons.get(0).getAddresses().size());

        em.remove(person);

        person = em.find(Person1M.class, "person_1");
        Assert.assertNull(person);
        Assert.assertNull(em.find(Address1M.class, "expressway"));
        Assert.assertNull(em.find(Address1M.class, "noida street"));

    }

    @Test
    public void testCRUDM1() throws InterruptedException
    {
        PersonM1 person1 = new PersonM1();
        person1.setPersonId("person_1");
        person1.setPersonName("vivek");

        PersonM1 person2 = new PersonM1();
        person2.setPersonId("person_2");
        person2.setPersonName("vivek");

        Set<PersonM1> persons = new HashSet<PersonM1>();
        persons.add(person1);
        persons.add(person1);

        AddressM1 address = new AddressM1();
        address.setAddressId("noidastreet");
        address.setPersons(persons);
        address.setStreet("Sector 50");

        person1.setAddresses(address);

        person2.setAddresses(address);

        em.persist(person1);
        em.persist(person2);
        // em.persist(address);

        waitThread();
        em.clear();

        person1 = em.find(PersonM1.class, "person_1");
        Assert.assertNotNull(person1);
        Assert.assertNotNull(person1.getAddresses());
        Assert.assertEquals(2, person1.getAddresses().getPersons().size());

        Query q = em.createQuery("select p from PersonM1 p where p.personName = vivek");
        List<PersonM1> foundPersons = q.getResultList();
        Assert.assertNotNull(foundPersons);
        Assert.assertEquals(2, foundPersons.size());
        Assert.assertNotNull(foundPersons.get(0).getAddresses());
        Assert.assertEquals("Sector 50", foundPersons.get(0).getAddresses().getStreet());
        Assert.assertNotNull(foundPersons.get(1).getAddresses());
        Assert.assertEquals("Sector 50", foundPersons.get(1).getAddresses().getStreet());

        em.remove(person1);
        em.remove(person2);

    }
    
    @After
    public void tearDown()
    {
        if(em != null)
        {
            em.close();
        }
        
        if(emf != null)
        {
            emf.close();
        }
        

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        node.close();
    }

 
    private void waitThread() throws InterruptedException
    {
        Thread.sleep(2000);
    }
}