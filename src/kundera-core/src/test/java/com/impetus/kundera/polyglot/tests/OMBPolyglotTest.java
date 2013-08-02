/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.polyglot.tests;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.polyglot.entities.AddressB1M;
import com.impetus.kundera.polyglot.entities.PersonB1M;

public class OMBPolyglotTest extends PersonAddressTestBase
{

    @Before
    public void setUp() throws Exception
    {   
        super.init();
    }

    /**
     * Test insert.
     */
    @Test
    public void testCRUD()
    {
       executeAllTests();
    }

    @Override
    protected void insert()
    {
        PersonB1M personnel = new PersonB1M();
        personnel.setPersonId("bionetomany_1");
        personnel.setPersonName("Amresh");

        Set<AddressB1M> addresses = new HashSet<AddressB1M>();
        AddressB1M address1 = new AddressB1M();
        address1.setAddressId("bionetomany_a");
        address1.setStreet("AAAAAAAAAAAAA");

        AddressB1M address2 = new AddressB1M();
        address2.setAddressId("bionetomany_b");
        address2.setStreet("BBBBBBBBBBB");

        addresses.add(address1);
        addresses.add(address2);
        personnel.setAddresses(addresses);
        dao.insert(personnel);
     }

    @Override
    protected void find()
    {
        // Find Person
        PersonB1M p = (PersonB1M) dao.findPerson(PersonB1M.class, "bionetomany_1");
        assertPerson(p);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonB1M p = (PersonB1M) dao.findPersonByIdColumn(PersonB1M.class, "bionetomany_1");
        assertPerson(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonB1M> persons = dao.findPersonByName(PersonB1M.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonB1M person = persons.get(0);
        assertPerson(person);
    }

    @Override
    protected void findAddressByIdColumn()
    {
    }

    @Override
    protected void findAddressByStreet()
    {
    }

    @Override
    protected void update()
    {
        // Find Person
        PersonB1M p = (PersonB1M) dao.findPerson(PersonB1M.class, "bionetomany_1");
        assertPerson(p);

        p.setPersonName("Saurabh");
        for (AddressB1M address : p.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        dao.merge(p);
        PersonB1M pAfterMerge = (PersonB1M) dao.findPerson(PersonB1M.class, "bionetomany_1");

        assertPersonAfterUpdate(pAfterMerge);
    }

    @Override
    protected void remove()
    {
        // Find Person
        PersonB1M p = (PersonB1M) dao.findPerson(PersonB1M.class, "bionetomany_1");
        assertPersonAfterUpdate(p);

        dao.remove("bionetomany_1", PersonB1M.class);
        PersonB1M pAfterRemoval = (PersonB1M) dao.findPerson(PersonB1M.class, "bionetomany_1");
        Assert.assertNull(pAfterRemoval);
    }

    private void assertPerson(PersonB1M p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("bionetomany_1", p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        Set<AddressB1M> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (AddressB1M address : adds)
        {
            Assert.assertNotNull(address);
            PersonB1M person = address.getPerson();
            /*Assert.assertNotNull(person);
            Assert.assertEquals(p.getPersonId(), person.getPersonId());
            Assert.assertEquals(p.getPersonName(), person.getPersonName());
            Assert.assertNotNull(person.getAddresses());
            Assert.assertFalse(person.getAddresses().isEmpty());
            Assert.assertEquals(2, person.getAddresses().size());*/
        }
    }

    private void assertPersonAfterUpdate(PersonB1M p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("bionetomany_1", p.getPersonId());
        Assert.assertEquals("Saurabh", p.getPersonName());

        Set<AddressB1M> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (AddressB1M address : adds)
        {
            Assert.assertNotNull(address);
            Assert.assertEquals("Brand New Street", address.getStreet());
            PersonB1M person = address.getPerson();
            /*Assert.assertNotNull(person);
            Assert.assertEquals(p.getPersonId(), person.getPersonId());
            Assert.assertEquals(p.getPersonName(), person.getPersonName());
            Assert.assertNotNull(person.getAddresses());
            Assert.assertFalse(person.getAddresses().isEmpty());
            Assert.assertEquals(2, person.getAddresses().size());*/
        }
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
      super.close();
    }  
}
