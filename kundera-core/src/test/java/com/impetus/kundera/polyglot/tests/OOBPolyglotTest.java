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

import com.impetus.kundera.polyglot.entities.AddressB11FK;
import com.impetus.kundera.polyglot.entities.PersonB11FK;

public class OOBPolyglotTest extends PersonAddressTestBase
{

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

    @Override
    protected void insert()
    {
        PersonB11FK person = new PersonB11FK();
        person.setPersonId("bionetoonefk_1");
        person.setPersonName("Amresh");

        AddressB11FK address = new AddressB11FK();
        address.setAddressId("bionetoonefk_a");
        address.setStreet("123, New street");
        person.setAddress(address);
        address.setPerson(person);

        dao.insert(person);
    }

    @Override
    protected void find()
    {
        // Find Person
        PersonB11FK p = (PersonB11FK) dao.findPerson(PersonB11FK.class, "bionetoonefk_1");
        assertPersonnel(p);       
 
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonB11FK p = (PersonB11FK) dao.findPersonByIdColumn(PersonB11FK.class, "bionetoonefk_1");
        assertPersonnel(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonB11FK> persons = dao.findPersonByName(PersonB11FK.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        assertPersonnel(persons.get(0));
    }

    @Override
    protected void findAddressByIdColumn()
    {
        /*
         * AddressB11FK a = (AddressB11FK)
         * dao.findAddressByIdColumn(AddressB11FK.class, "bionetoonefk_a");
         * assertAddress(a);
         */
    }

    @Override
    protected void findAddressByStreet()
    {
        /*
         * List<AddressB11FK> adds =
         * dao.findAddressByStreet(AddressB11FK.class, "123, New street");
         * Assert.assertNotNull(adds); Assert.assertFalse(adds.isEmpty());
         * Assert.assertTrue(adds.size() == 1);
         * 
         * assertAddress(adds.get(0));
         */
    }

    @Override
    protected void update()
    {
        try
        {
            PersonB11FK p = (PersonB11FK) dao.findPerson(PersonB11FK.class, "bionetoonefk_1");
            assertPersonnel(p);

            dao.merge(p); // This merge operation should do nothing since
                          // nothing has changed

            p = (PersonB11FK) dao.findPerson(PersonB11FK.class, "bionetoonefk_1");
            assertPersonnel(p);

            p.setPersonName("Saurabh");
            p.getAddress().setStreet("Brand New Street");
            dao.merge(p);

            PersonB11FK pAfterMerge = (PersonB11FK) dao.findPerson(PersonB11FK.class,
                    "bionetoonefk_1");
            assertPersonnelAfterUpdate(pAfterMerge);

        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Override
    protected void remove()
    {
        PersonB11FK p = (PersonB11FK) dao.findPerson(PersonB11FK.class, "bionetoonefk_1");
        assertPersonnelAfterUpdate(p);

        dao.remove("bionetoonefk_1", PersonB11FK.class);

        PersonB11FK pAfterRemoval = (PersonB11FK) dao.findPerson(PersonB11FK.class, "bionetoonefk_1");
        Assert.assertNull(pAfterRemoval);
    }

    private void assertPersonnel(PersonB11FK p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("bionetoonefk_1", p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        AddressB11FK address = p.getAddress();       
        assertAddress(address);
    }

    /**
     * @param address
     */
    private void assertAddress(AddressB11FK address)
    {
        Assert.assertNotNull(address);
        Assert.assertEquals("bionetoonefk_a", address.getAddressId());
        Assert.assertEquals("123, New street", address.getStreet());

        PersonB11FK pp = address.getPerson();
        Assert.assertNotNull(pp);
        Assert.assertEquals("bionetoonefk_1", pp.getPersonId());
        Assert.assertEquals("Amresh", pp.getPersonName());
    }

    private void assertPersonnelAfterUpdate(PersonB11FK pAfterMerge)
    {
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());
        AddressB11FK addressAfterMerge = pAfterMerge.getAddress();
        Assert.assertNotNull(addressAfterMerge);
        Assert.assertEquals("Brand New Street", addressAfterMerge.getStreet());

        PersonB11FK pp = addressAfterMerge.getPerson();
        Assert.assertNotNull(pp);
        Assert.assertEquals("bionetoonefk_1", pp.getPersonId());
        Assert.assertEquals("Saurabh", pp.getPersonName());
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
