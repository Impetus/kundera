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

import com.impetus.kundera.polyglot.entities.AddressU1M;
import com.impetus.kundera.polyglot.entities.PersonU1M;

/**
 * @author vivek.mishra
 * 
 */
public class OMUPolyglotTest extends PersonAddressTestBase
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
        // Save Person
        PersonU1M personnel = new PersonU1M();
        personnel.setPersonId("unionetomany_1");
        personnel.setPersonName("Amresh");

        Set<AddressU1M> addresses = new HashSet<AddressU1M>();
        AddressU1M address1 = new AddressU1M();
        address1.setAddressId("unionetomany_a");
        address1.setStreet("AAAAAAAAAAAAA");

        AddressU1M address2 = new AddressU1M();
        address2.setAddressId("unionetomany_b");
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
        PersonU1M p = (PersonU1M) dao.findPerson(PersonU1M.class, "unionetomany_1");
        assertPerson(p);
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonU1M p = (PersonU1M) dao.findPersonByIdColumn(PersonU1M.class, "unionetomany_1");
        assertPerson(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonU1M> persons = dao.findPersonByName(PersonU1M.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonU1M person = persons.get(0);
        assertPerson(person);
    }

    @Override
    protected void findAddressByIdColumn()
    {
        AddressU1M a = (AddressU1M) dao.findAddressByIdColumn(AddressU1M.class, "unionetomany_a");
        Assert.assertNotNull(a);
        Assert.assertEquals("unionetomany_a", a.getAddressId());
        Assert.assertEquals("AAAAAAAAAAAAA", a.getStreet());
    }

    @Override
    protected void findAddressByStreet()
    {
        List<AddressU1M> adds = dao.findAddressByStreet(AddressU1M.class, "AAAAAAAAAAAAA");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        AddressU1M a = adds.get(0);
        Assert.assertNotNull(a);
        Assert.assertEquals("unionetomany_a", a.getAddressId());
        Assert.assertEquals("AAAAAAAAAAAAA", a.getStreet());
    }

    @Override
    protected void update()
    {
        try
        {
            PersonU1M p = (PersonU1M) dao.findPerson(PersonU1M.class, "unionetomany_1");
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getAddresses().size());
            p.setPersonName("Saurabh");

            for (AddressU1M address : p.getAddresses())
            {
                address.setStreet("Brand New Street");
            }
            dao.merge(p);
            assertPersonAfterUpdate();
        }
        catch (Exception e)
        {
            
            Assert.fail();
        }
    }

    @Override
    protected void remove()
    {
        try
        {
            // PersonU1M p = (PersonU1M)
            // dao.findPerson(PersonU1M.class, );
            dao.remove("unionetomany_1", PersonU1M.class);
            PersonU1M pAfterRemoval = (PersonU1M) dao
                    .findPerson(PersonU1M.class, "unionetomany_1");
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
    private void assertPerson(PersonU1M p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("unionetomany_1", p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        Set<AddressU1M> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (AddressU1M address : adds)
        {
            Assert.assertNotNull(address.getStreet());
        }
    }

    /**
     * 
     */
    private void assertPersonAfterUpdate()
    {
        PersonU1M pAfterMerge = (PersonU1M) dao.findPerson(PersonU1M.class, "unionetomany_1");
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());
        Assert.assertEquals(2, pAfterMerge.getAddresses().size());

        for (AddressU1M address : pAfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }
    }  

}
