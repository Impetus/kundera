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
import org.junit.Test;

import com.impetus.kundera.polyglot.entities.AddressBMM;
import com.impetus.kundera.polyglot.entities.PersonBMM;

public class MMBPolyglotTest extends PersonAddressTestBase
{

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
      super.init();
    }

    @Test
    public void testCRUD()
    {
       executeAllTests();
    }

    @Override
    protected void insert()
    {
        PersonBMM person1 = new PersonBMM();
        person1.setPersonId("bimanytomany_1");
        person1.setPersonName("Amresh");

        PersonBMM person2 = new PersonBMM();
        person2.setPersonId("bimanytomany_2");
        person2.setPersonName("Vivek");

        AddressBMM address1 = new AddressBMM();
        address1.setAddressId("bimanytomany_a");
        address1.setStreet("AAAAAAAAAAAAA");

        AddressBMM address2 = new AddressBMM();
        address2.setAddressId("bimanytomany_b");
        address2.setStreet("BBBBBBBBBBBBBBB");

        AddressBMM address3 = new AddressBMM();
        address3.setAddressId("bimanytomany_c");
        address3.setStreet("CCCCCCCCCCC");

        Set<AddressBMM> person1Addresses = new HashSet<AddressBMM>();
        Set<AddressBMM> person2Addresses = new HashSet<AddressBMM>();

        person1Addresses.add(address1);
        person1Addresses.add(address2);

        person2Addresses.add(address2);
        person2Addresses.add(address3);

        person1.setAddresses(person1Addresses);
        person2.setAddresses(person2Addresses);

        Set<PersonBMM> persons = new HashSet<PersonBMM>();
        persons.add(person1);
        persons.add(person2);

        dao.savePersons(persons);
     }

    @Override
    protected void find()
    {
        PersonBMM person1 = (PersonBMM) dao.findPerson(PersonBMM.class, "bimanytomany_1");
        assertPerson1(person1);

        PersonBMM person2 = (PersonBMM) dao.findPerson(PersonBMM.class, "bimanytomany_2");
        assertPerson2(person2);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        // Find Person 1
        PersonBMM p1 = (PersonBMM) dao.findPersonByIdColumn(PersonBMM.class, "bimanytomany_1");
        assertPerson1(p1);

        // Find Person 2
        PersonBMM p2 = (PersonBMM) dao.findPersonByIdColumn(PersonBMM.class, "bimanytomany_2");
        assertPerson2(p2);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonBMM> persons = dao.findPersonByName(PersonBMM.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        assertPerson1(persons.get(0));
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
    }

    @Override
    protected void remove()
    {
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

    /**
     * @param person2
     */
    private void assertPerson2(PersonBMM person2)
    {
        Assert.assertNotNull(person2);

        Assert.assertEquals("bimanytomany_2", person2.getPersonId());
        Assert.assertEquals("Vivek", person2.getPersonName());

        Set<AddressBMM> addresses2 = person2.getAddresses();
        Assert.assertNotNull(addresses2);
        Assert.assertFalse(addresses2.isEmpty());
        Assert.assertEquals(2, addresses2.size());
        AddressBMM address21 = (AddressBMM) addresses2.toArray()[0];
        Assert.assertNotNull(address21);
        AddressBMM address22 = (AddressBMM) addresses2.toArray()[1];
        Assert.assertNotNull(address22);
    }

    /**
     * @param person1
     */
    private void assertPerson1(PersonBMM person1)
    {
        Assert.assertNotNull(person1);
        Assert.assertEquals("bimanytomany_1", person1.getPersonId());
        Assert.assertEquals("Amresh", person1.getPersonName());

        Set<AddressBMM> addresses1 = person1.getAddresses();
        Assert.assertNotNull(addresses1);
        Assert.assertFalse(addresses1.isEmpty());
        Assert.assertEquals(2, addresses1.size());
        AddressBMM address11 = (AddressBMM) addresses1.toArray()[0];
        Assert.assertNotNull(address11);
        /*Assert.assertNotNull(address11.getPeople());
        Assert.assertFalse(address11.getPeople().isEmpty());
        AddressBMM address12 = (AddressBMM) addresses1.toArray()[1];
        Assert.assertNotNull(address12.getPeople());
        Assert.assertFalse(address12.getPeople().isEmpty());*/
    }  

}
