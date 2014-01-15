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

import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.polyglot.entities.AddressU11FK;
import com.impetus.kundera.polyglot.entities.PersonU11FK;

/**

 */
public class OOUPolyglotTest extends PersonAddressTestBase
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

    /**
     * Test CRUD
     */
    @Test
    public void testCRUD()
    {
       executeAllTests();
    }

    protected void insert()
    {
        try
        {
            PersonU11FK person = new PersonU11FK();
            AddressU11FK address = new AddressU11FK();
            person.setPersonId("unionetoonefk_1");
            person.setPersonName("Amresh");
            address.setAddressId("unionetoonefk_a");
            address.setStreet("123, New street");
            person.setAddress(address);
            dao.insert(person);
        }
        catch (Exception e)
        {
            
            Assert.fail();
        }
    }

    protected void find()
    {
        // Find Person
        try
        {
            PersonU11FK p = (PersonU11FK) dao.findPerson(PersonU11FK.class, "unionetoonefk_1");
            assertPersonBeforeUpdate(p);
        }
        catch (Exception e)
        {
            
            Assert.fail();
        }
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonU11FK p = (PersonU11FK) dao.findPersonByIdColumn(PersonU11FK.class,
                "unionetoonefk_1");
        assertPersonBeforeUpdate(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonU11FK> persons = dao.findPersonByName(PersonU11FK.class, "Amresh");
        assertPersons(persons);
    }

    @Override
    protected void findAddressByIdColumn()
    {
        AddressU11FK a = (AddressU11FK) dao.findAddressByIdColumn(AddressU11FK.class, "unionetoonefk_a");
        assertAddressBeforeUpdate(a);
    }

    @Override
    protected void findAddressByStreet()
    {
        List<AddressU11FK> adds = dao.findAddressByStreet(AddressU11FK.class, "123, New street");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        assertAddressBeforeUpdate(adds.get(0));
    }

    protected void update()
    {
        try
        {
            PersonU11FK p = (PersonU11FK) dao.findPerson(PersonU11FK.class, "unionetoonefk_1");
            Assert.assertNotNull(p);
            p.setPersonName("Saurabh");
            AddressU11FK address = p.getAddress();
            address.setStreet("Brand New Street");
            p.setAddress(address);
            dao.merge(p);

            PersonU11FK pAfterMerge = (PersonU11FK) dao.findPerson(PersonU11FK.class,
                    "unionetoonefk_1");
            assertPersonAfterUpdate(pAfterMerge);
        }
        catch (Exception e)
        {
            
            Assert.fail();
        }
    }

    protected void remove()
    {
        try
        {
            PersonU11FK p = (PersonU11FK) dao.findPerson(PersonU11FK.class, "unionetoonefk_1");
            Assert.assertNotNull(p);
            dao.remove("unionetoonefk_1", PersonU11FK.class);

            PersonU11FK pAfterRemoval = (PersonU11FK) dao.findPerson(PersonU11FK.class,
                    "unionetoonefk_1");
            Assert.assertNull(pAfterRemoval);
        }
        catch (Exception e)
        {
            
            Assert.fail();
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


    /**
     * @param p
     */
    private void assertPersonBeforeUpdate(PersonU11FK p)
    {
        Assert.assertNotNull(p);

        Assert.assertEquals("unionetoonefk_1", p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        assertAddressBeforeUpdate(p.getAddress());
    }

    /**
     * @param p
     */
    private void assertAddressBeforeUpdate(AddressU11FK add)
    {
        Assert.assertNotNull(add);

        String addressId = add.getAddressId();
        String street = add.getStreet();

        Assert.assertNotNull(addressId);
        Assert.assertEquals("unionetoonefk_a", addressId);
        Assert.assertEquals("123, New street", street);
    }

    /**
     * @param pAfterMerge
     */
    private void assertPersonAfterUpdate(PersonU11FK pAfterMerge)
    {
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());
        AddressU11FK addressAfterMerge = pAfterMerge.getAddress();
        Assert.assertNotNull(addressAfterMerge);
        Assert.assertEquals("Brand New Street", addressAfterMerge.getStreet());
    }

    /**
     * @param persons
     */
    private void assertPersons(List<PersonU11FK> persons)
    {
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonU11FK person = persons.get(0);
        assertPersonBeforeUpdate(person);
    }
}
