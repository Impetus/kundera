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

import com.impetus.kundera.polyglot.entities.AddressUMM;
import com.impetus.kundera.polyglot.entities.PersonUMM;

/**
 * @author vivek.mishra
 * 
 */
public class MMUPolyglotTest extends PersonAddressTestBase
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
        PersonUMM person1 = new PersonUMM();
        person1.setPersonId("unimanytomany_1");
        person1.setPersonName("Amresh");

        PersonUMM person2 = new PersonUMM();
        person2.setPersonId("unimanytomany_2");
        person2.setPersonName("Vivek");

        AddressUMM address1 = new AddressUMM();
        address1.setAddressId("unimanytomany_a");
        address1.setStreet("AAAAAAAAAAAAA");

        AddressUMM address2 = new AddressUMM();
        address2.setAddressId("unimanytomany_b");
        address2.setStreet("BBBBBBBBBBBBBBB");

        AddressUMM address3 = new AddressUMM();
        address3.setAddressId("unimanytomany_c");
        address3.setStreet("CCCCCCCCCCC");

        Set<AddressUMM> person1Addresses = new HashSet<AddressUMM>();
        Set<AddressUMM> person2Addresses = new HashSet<AddressUMM>();

        person1Addresses.add(address1);
        person1Addresses.add(address2);

        person2Addresses.add(address2);
        person2Addresses.add(address3);

        person1.setAddresses(person1Addresses);
        person2.setAddresses(person2Addresses);

        Set<PersonUMM> persons = new HashSet<PersonUMM>();
        persons.add(person1);
        persons.add(person2);

        dao.savePersons(persons);
      
    }

    @Override
    protected void find()
    {

        PersonUMM person1 = (PersonUMM) dao.findPerson(PersonUMM.class, "unimanytomany_1");
        assertPerson1(person1);

        PersonUMM person2 = (PersonUMM) dao.findPerson(PersonUMM.class, "unimanytomany_2");
        assertPerson2(person2);
    }

    @Override
    protected void findPersonByIdColumn()
    {
        // Find Person 1
        PersonUMM p1 = (PersonUMM) dao.findPersonByIdColumn(PersonUMM.class, "unimanytomany_1");
        assertPerson1(p1);

        // Find Person 2
        PersonUMM p2 = (PersonUMM) dao.findPersonByIdColumn(PersonUMM.class, "unimanytomany_2");
        assertPerson2(p2);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonUMM> persons = dao.findPersonByName(PersonUMM.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        assertPerson1(persons.get(0));
    }

    @Override
    protected void findAddressByIdColumn()
    {
        AddressUMM a = (AddressUMM) dao.findAddressByIdColumn(AddressUMM.class, "unimanytomany_a");
        assertAddressForPerson1(a);
    }

    @Override
    protected void findAddressByStreet()
    {
        List<AddressUMM> adds = dao.findAddressByStreet(AddressUMM.class, "AAAAAAAAAAAAA");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        assertAddressForPerson1(adds.get(0));
    }

    @Override
    protected void update()
    {
        PersonUMM p1 = (PersonUMM) dao.findPerson(PersonUMM.class, "unimanytomany_1");
        Assert.assertNotNull(p1);
        p1.setPersonName("Saurabh");
        Assert.assertEquals(2, p1.getAddresses().size());
        for (AddressUMM address : p1.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        dao.merge(p1);
        PersonUMM p1AfterMerge = (PersonUMM) dao.findPerson(PersonUMM.class, "unimanytomany_1");
        Assert.assertNotNull(p1AfterMerge);
        Assert.assertEquals("Saurabh", p1AfterMerge.getPersonName());
        Assert.assertEquals(2, p1AfterMerge.getAddresses().size());
        for (AddressUMM address : p1AfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }

        PersonUMM p2 = (PersonUMM) dao.findPerson(PersonUMM.class, "unimanytomany_2");
        Assert.assertNotNull(p2);
        Assert.assertEquals(2, p2.getAddresses().size());
        p2.setPersonName("Vijay");
        for (AddressUMM address : p2.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        dao.merge(p2);
        PersonUMM p2AfterMerge = (PersonUMM) dao.findPerson(PersonUMM.class, "unimanytomany_2");
        Assert.assertNotNull(p2AfterMerge);
        Assert.assertEquals("Vijay", p2AfterMerge.getPersonName());
        Assert.assertEquals(2, p2AfterMerge.getAddresses().size());
        for (AddressUMM address : p2AfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }
    }

    @Override
    protected void remove()
    {
        dao.remove("unimanytomany_1", PersonUMM.class);
        PersonUMM person1AfterRemoval = (PersonUMM) dao.findPerson(PersonUMM.class,
                "unimanytomany_1");
        Assert.assertNull(person1AfterRemoval);

        dao.remove("unimanytomany_2", PersonUMM.class);
        PersonUMM person2AfterRemoval = (PersonUMM) dao.findPerson(PersonUMM.class,
                "unimanytomany_2");
        Assert.assertNull(person2AfterRemoval);
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
     * @param person1
     */
    private void assertPerson1(PersonUMM person1)
    {
        Assert.assertNotNull(person1);
        Assert.assertEquals("unimanytomany_1", person1.getPersonId());
        Assert.assertEquals("Amresh", person1.getPersonName());

        Set<AddressUMM> addresses1 = person1.getAddresses();
        Assert.assertNotNull(addresses1);
        Assert.assertFalse(addresses1.isEmpty());
        Assert.assertEquals(2, addresses1.size());
        AddressUMM address11 = (AddressUMM) addresses1.toArray()[0];
        assertAddressForPerson1(address11);
        AddressUMM address12 = (AddressUMM) addresses1.toArray()[1];
        assertAddressForPerson1(address12);
    }

    /**
     * @param person2
     */
    private void assertPerson2(PersonUMM person2)
    {
        Assert.assertNotNull(person2);

        Assert.assertEquals("unimanytomany_2", person2.getPersonId());
        Assert.assertEquals("Vivek", person2.getPersonName());

        Set<AddressUMM> addresses2 = person2.getAddresses();
        Assert.assertNotNull(addresses2);
        Assert.assertFalse(addresses2.isEmpty());
        Assert.assertEquals(2, addresses2.size());
        AddressUMM address21 = (AddressUMM) addresses2.toArray()[0];
        assertAddressForPerson2(address21);
        AddressUMM address22 = (AddressUMM) addresses2.toArray()[1];
        assertAddressForPerson2(address22);
    }

    /**
     * @param address11
     */
    private void assertAddressForPerson1(AddressUMM address)
    {
        Assert.assertNotNull(address);
        Assert.assertTrue("unimanytomany_a".equals(address.getAddressId())
                || "unimanytomany_b".equals(address.getAddressId()));
        Assert.assertTrue("AAAAAAAAAAAAA".equals(address.getStreet()) || "BBBBBBBBBBBBBBB".equals(address.getStreet()));
    }

    /**
     * @param address12
     */
    private void assertAddressForPerson2(AddressUMM address)
    {
        Assert.assertNotNull(address);

        Assert.assertTrue("unimanytomany_b".equals(address.getAddressId())
                || "unimanytomany_b".equals(address.getAddressId()) || "unimanytomany_c".equals(address.getAddressId()));
        Assert.assertTrue("CCCCCCCCCCC".equals(address.getStreet()) || "BBBBBBBBBBBBBBB".equals(address.getStreet()));

    }  

}
