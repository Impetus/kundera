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

import com.impetus.kundera.polyglot.entities.AddressBM1;
import com.impetus.kundera.polyglot.entities.PersonBM1;

public class MOBPolyglotTest extends PersonAddressTestBase
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
        PersonBM1 person1 = new PersonBM1();
        person1.setPersonId("bimanytoone_1");
        person1.setPersonName("Amresh");

        PersonBM1 person2 = new PersonBM1();
        person2.setPersonId("bimanytoone_2");
        person2.setPersonName("Vivek");

        AddressBM1 address = new AddressBM1();
        address.setAddressId("bimanytoone_b");
        address.setStreet("AAAAAAAAAAAAA");

        person1.setAddress(address);
        person2.setAddress(address);

        Set<PersonBM1> persons = new HashSet<PersonBM1>();
        persons.add(person1);
        persons.add(person2);

        dao.savePersons(persons);
    }

    @Override
    protected void find()
    {
        // Find Person 1
        PersonBM1 p1 = (PersonBM1) dao.findPerson(PersonBM1.class, "bimanytoone_1");
        assertPerson1(p1);

        // Find Person 2
        PersonBM1 p2 = (PersonBM1) dao.findPerson(PersonBM1.class, "bimanytoone_2");
        assertPerson2(p2);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonBM1 p = (PersonBM1) dao.findPersonByIdColumn(PersonBM1.class, "bimanytoone_1");
        assertPerson1(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonBM1> persons = dao.findPersonByName(PersonBM1.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonBM1 person = persons.get(0);
        assertPerson1(person);
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
     * @param p2
     */
    private void assertPerson2(PersonBM1 p2)
    {
        Assert.assertNotNull(p2);
        Assert.assertEquals("bimanytoone_2", p2.getPersonId());
        Assert.assertEquals("Vivek", p2.getPersonName());

        AddressBM1 add2 = p2.getAddress();
        Assert.assertNotNull(add2);

        Assert.assertEquals("bimanytoone_b", add2.getAddressId());
        Set<PersonBM1> people2 = add2.getPeople();
        /*Assert.assertNotNull(people2);
        Assert.assertFalse(people2.isEmpty());
        Assert.assertEquals(2, people2.size());*/
    }

    /**
     * @param p1
     */
    private void assertPerson1(PersonBM1 p1)
    {
        Assert.assertNotNull(p1);
        Assert.assertEquals("bimanytoone_1", p1.getPersonId());
        Assert.assertEquals("Amresh", p1.getPersonName());

        AddressBM1 add = p1.getAddress();
        Assert.assertNotNull(add);

        Assert.assertEquals("bimanytoone_b", add.getAddressId());
        Set<PersonBM1> people = add.getPeople();
        /*Assert.assertNotNull(people);
        Assert.assertFalse(people.isEmpty());
        Assert.assertEquals(2, people.size());*/
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
