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

import org.junit.Before;
import org.junit.Test;

public class ManyToManyAssociationTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory("es-pu");
        em = emf.createEntityManager();
    }

    @Test
    public void testUniMTM()
    {
        PersonMM person1 = new PersonMM();
        person1.setPersonId("1_p");
        person1.setPersonName("vivek");

        PersonMM person2 = new PersonMM();
        person2.setPersonId("2_p");
        person2.setPersonName("vivek");

        Set<AddressMM> addressess = new HashSet<AddressMM>();

        AddressMM address1 = new AddressMM();
        address1.setAddressId("addr_1");
        address1.setStreet("Noida");

        AddressMM address2 = new AddressMM();
        address2.setAddressId("addr_2");
        address2.setStreet("Noida");

        addressess.add(address1);
        addressess.add(address2);

        person1.setAddresses(addressess);
        person2.setAddresses(addressess);

        em.persist(person1);

        em.clear();

        em.persist(person2);

        waitThread();

        em.clear();

        PersonMM result = em.find(PersonMM.class, "1_p");
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getAddresses());
        Assert.assertEquals(2, result.getAddresses().size());

        em.clear();

        Assert.assertNotNull(em.find(AddressMM.class, "addr_1"));
        Assert.assertNotNull(em.find(AddressMM.class, "addr_2"));

        Query q = em.createQuery("select p from PersonMM p where p.personName = vivek");
        List<PersonMM> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        Assert.assertNotNull(persons.get(0).getAddresses());
        Assert.assertEquals(2, persons.get(0).getAddresses().size());
        Assert.assertNotNull(persons.get(1).getAddresses());
        Assert.assertEquals(2, persons.get(1).getAddresses().size());

        em.remove(result);

        em.clear();
        result = em.find(PersonMM.class, "1_p");
        Assert.assertNull(result);

        Assert.assertNull(em.find(AddressMM.class, "addr_1"));
        Assert.assertNull(em.find(AddressMM.class, "addr_2"));

    }

    @Test
    public void testBiMTM()
    {
        PersonBiMM person1 = new PersonBiMM();
        person1.setPersonId("1_p");
        person1.setPersonName("vivek");

        PersonBiMM person2 = new PersonBiMM();
        person2.setPersonId("2_p");
        person2.setPersonName("vivek");

        Set<AddressBiMM> addressess = new HashSet<AddressBiMM>();

        AddressBiMM address1 = new AddressBiMM();
        address1.setAddressId("addr_1");
        address1.setStreet("Noida");

        AddressBiMM address2 = new AddressBiMM();
        address2.setAddressId("addr_2");
        address2.setStreet("Noida");

        addressess.add(address1);
        addressess.add(address2);

        person1.setAddresses(addressess);
        person2.setAddresses(addressess);

        em.persist(person1);

        em.clear();

        em.persist(person2);

        waitThread();

        em.clear();

        PersonBiMM result = em.find(PersonBiMM.class, "1_p");
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getAddresses());
        Assert.assertEquals(2, result.getAddresses().size());

        Assert.assertEquals(2, result.getAddresses().iterator().next().getPeople().size());
        Assert.assertEquals(2, result.getAddresses().iterator().next().getPeople().size());

        em.clear();

        Assert.assertNotNull(em.find(AddressBiMM.class, "addr_1"));
        Assert.assertNotNull(em.find(AddressBiMM.class, "addr_2"));

        Query q = em.createQuery("select p from PersonBiMM p where p.personName = vivek");
        List<PersonBiMM> persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(2, persons.size());
        Assert.assertNotNull(persons.get(0).getAddresses());
        Assert.assertEquals(2, persons.get(0).getAddresses().size());
        Assert.assertNotNull(persons.get(1).getAddresses());
        Assert.assertEquals(2, persons.get(1).getAddresses().size());

        em.remove(result);

        em.clear();
        result = em.find(PersonBiMM.class, "1_p");
        Assert.assertNull(result);

        Assert.assertNull(em.find(AddressBiMM.class, "addr_1"));
        Assert.assertNull(em.find(AddressBiMM.class, "addr_2"));

    }

    private void waitThread()
    {
        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

}