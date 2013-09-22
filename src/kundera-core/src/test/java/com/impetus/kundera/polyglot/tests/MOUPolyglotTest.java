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

import com.impetus.kundera.polyglot.entities.AddressUM1;
import com.impetus.kundera.polyglot.entities.PersonUM1;

/**
 * @author vivek.mishra
 * 
 */
public class MOUPolyglotTest extends PersonAddressTestBase
{

    @Before
    public void setUp() throws Exception
    {
      super.init();
    }

    /**
     * Test CRUD.
     */
    @Test
    public void testCRUD()
    {
      executeAllTests();
    }

    @Override
    protected void insert()
    {
        PersonUM1 person1 = new PersonUM1();
        person1.setPersonId("unimanytoone_1");
        person1.setPersonName("Amresh");

        PersonUM1 person2 = new PersonUM1();
        person2.setPersonId("unimanytoone_2");
        person2.setPersonName("Vivek");

        AddressUM1 address = new AddressUM1();
        address.setAddressId("unimanytoone_a");
        address.setStreet("AAAAAAAAAAAAA");

        person1.setAddress(address);
        person2.setAddress(address);

        Set<PersonUM1> persons = new HashSet<PersonUM1>();
        persons.add(person1);
        persons.add(person2);

        dao.savePersons(persons);
    }

    @Override
    protected void find()
    {
        // Find Person 1
        PersonUM1 p1 = (PersonUM1) dao.findPerson(PersonUM1.class, "unimanytoone_1");
        assertPerson1(p1);

        // Find Person 2
        PersonUM1 p2 = (PersonUM1) dao.findPerson(PersonUM1.class, "unimanytoone_2");
        assertPerson2(p2);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonUM1 p = (PersonUM1) dao.findPersonByIdColumn(PersonUM1.class, "unimanytoone_1");
        assertPerson1(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonUM1> persons = dao.findPersonByName(PersonUM1.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonUM1 person = persons.get(0);
        assertPerson1(person);
    }

    @Override
    protected void findAddressByIdColumn()
    {
        AddressUM1 a = (AddressUM1) dao.findAddressByIdColumn(AddressUM1.class, "unimanytoone_a");
        assertAddress(a);
    }

    @Override
    protected void findAddressByStreet()
    {
        List<AddressUM1> adds = dao.findAddressByStreet(AddressUM1.class, "AAAAAAAAAAAAA");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        assertAddress(adds.get(0));
    }

    @Override
    protected void update()
    {
        // Find Person 1
        PersonUM1 p1 = (PersonUM1) dao.findPerson(PersonUM1.class, "unimanytoone_1");
        Assert.assertNotNull(p1);
        p1.setPersonName("Saurabh");
        p1.getAddress().setStreet("Brand New Street");
        dao.merge(p1);
        PersonUM1 p1AfterMerge = (PersonUM1) dao.findPerson(PersonUM1.class, "unimanytoone_1");
        Assert.assertNotNull(p1AfterMerge);
        Assert.assertEquals("Saurabh", p1AfterMerge.getPersonName());
        Assert.assertEquals("Brand New Street", p1AfterMerge.getAddress().getStreet());

        // Find Person 2
        PersonUM1 p2 = (PersonUM1) dao.findPerson(PersonUM1.class, "unimanytoone_2");
        Assert.assertNotNull(p2);
        p2.setPersonName("Prateek");
        dao.merge(p2);
        PersonUM1 p2AfterMerge = (PersonUM1) dao.findPerson(PersonUM1.class, "unimanytoone_2");
        Assert.assertNotNull(p2AfterMerge);
        Assert.assertEquals("Prateek", p2AfterMerge.getPersonName());
    }

    @Override
    protected void remove()
    {
        // Remove Person 1
        dao.remove("unimanytoone_1", PersonUM1.class);
        PersonUM1 p1AfterRemoval = (PersonUM1) dao.findPerson(PersonUM1.class, "unimanytoone_1");
        Assert.assertNull(p1AfterRemoval);

        // Remove Person 2
        dao.remove("unimanytoone_2", PersonUM1.class);
        PersonUM1 p2AfterRemoval = (PersonUM1) dao.findPerson(PersonUM1.class, "unimanytoone_2");
        Assert.assertNull(p2AfterRemoval);
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
     * @param p2
     */
    private void assertPerson2(PersonUM1 p2)
    {
        Assert.assertNotNull(p2);
        Assert.assertEquals("unimanytoone_2", p2.getPersonId());
        Assert.assertEquals("Vivek", p2.getPersonName());

        AddressUM1 add2 = p2.getAddress();
        assertAddress(add2);
    }

    /**
     * @param p1
     */
    private void assertPerson1(PersonUM1 p1)
    {
        Assert.assertNotNull(p1);
        Assert.assertEquals("unimanytoone_1", p1.getPersonId());
        Assert.assertEquals("Amresh", p1.getPersonName());

        AddressUM1 add = p1.getAddress();
        assertAddress(add);
    }

    /**
     * @param add2
     */
    private void assertAddress(AddressUM1 add2)
    {
        Assert.assertNotNull(add2);

        Assert.assertEquals("unimanytoone_a", add2.getAddressId());
        Assert.assertEquals("AAAAAAAAAAAAA", add2.getStreet());
    }

   

}
