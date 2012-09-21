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
package com.impetus.kundera.tests.crossdatastore.useraddress.datatype;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.TwinAssociation;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.HabitatUniMToMBigInteger;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.PersonnelUniMToMInt;

/**
 * @author vivek.mishra
 * 
 */
public class MTMUniAssociationIntTest extends TwinAssociation
{
    private BigInteger addressID1 = BigInteger.TEN;

    private BigInteger addressID2 = BigInteger.ONE;

    private BigInteger addressID3 = new BigInteger("1234567");

    public static final String[] ALL_PUs_UNDER_TEST = new String[] { "addCassandra", "addMongo" };

    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            CassandraCli.cassandraSetUp();

        }
        else
        {
            if (AUTO_MANAGE_SCHEMA)
            {
                CassandraCli.initClient();
            }

        }
        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelUniMToMInt.class);
        clazzz.add(HabitatUniMToMBigInteger.class);
        init(clazzz, ALL_PUs_UNDER_TEST);

    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        setUpInternal("ADDRESS", "PERSONNEL");
    }

    /**
     * Test insert.
     */
    @Test
    public void testCRUD()
    {
        try
        {
            // tryOperation(ALL_PUs_UNDER_TEST);
        }
        catch (Exception e)
        {
            Assert.fail("Failed, Caused by:" + e.getMessage());
        }
    }

    @Override
    protected void insert()
    {
        PersonnelUniMToMInt person1 = new PersonnelUniMToMInt();
        person1.setPersonId(12345);
        person1.setPersonName("Amresh");

        PersonnelUniMToMInt person2 = new PersonnelUniMToMInt();
        person2.setPersonId(12346);
        person2.setPersonName("Vivek");

        HabitatUniMToMBigInteger address1 = new HabitatUniMToMBigInteger();
        address1.setAddressId(addressID1);
        address1.setStreet("AAAAAAAAAAAAA");

        HabitatUniMToMBigInteger address2 = new HabitatUniMToMBigInteger();
        address2.setAddressId(addressID2);
        address2.setStreet("BBBBBBBBBBBBBBB");

        HabitatUniMToMBigInteger address3 = new HabitatUniMToMBigInteger();
        address3.setAddressId(addressID3);
        address3.setStreet("CCCCCCCCCCC");

        Set<HabitatUniMToMBigInteger> person1Addresses = new HashSet<HabitatUniMToMBigInteger>();
        Set<HabitatUniMToMBigInteger> person2Addresses = new HashSet<HabitatUniMToMBigInteger>();

        person1Addresses.add(address1);
        person1Addresses.add(address2);

        person2Addresses.add(address2);
        person2Addresses.add(address3);

        person1.setAddresses(person1Addresses);
        person2.setAddresses(person2Addresses);

        Set<PersonnelUniMToMInt> persons = new HashSet<PersonnelUniMToMInt>();
        persons.add(person1);
        persons.add(person2);

        dao.savePersons(persons);

        col.add(person1);
        col.add(person2);
        col.add(address1);
        col.add(address2);
        col.add(address3);
    }

    @Override
    protected void find()
    {

        PersonnelUniMToMInt person1 = (PersonnelUniMToMInt) dao.findPerson(PersonnelUniMToMInt.class, 12345);
        assertPerson1(person1);

        PersonnelUniMToMInt person2 = (PersonnelUniMToMInt) dao.findPerson(PersonnelUniMToMInt.class, 12346);
        assertPerson2(person2);
    }

    @Override
    protected void findPersonByIdColumn()
    {
        // Find Person 1
        PersonnelUniMToMInt p1 = (PersonnelUniMToMInt) dao.findPersonByIdColumn(PersonnelUniMToMInt.class, 12345);
        assertPerson1(p1);

        // Find Person 2
        PersonnelUniMToMInt p2 = (PersonnelUniMToMInt) dao.findPersonByIdColumn(PersonnelUniMToMInt.class, 12346);
        assertPerson2(p2);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelUniMToMInt> persons = dao.findPersonByName(PersonnelUniMToMInt.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        assertPerson1(persons.get(0));
    }

    @Override
    protected void findAddressByIdColumn()
    {
        HabitatUniMToMBigInteger a = (HabitatUniMToMBigInteger) dao.findAddressByIdColumn(
                HabitatUniMToMBigInteger.class, addressID1);
        assertAddressForPerson1(a);
    }

    @Override
    protected void findAddressByStreet()
    {
        List<HabitatUniMToMBigInteger> adds = dao.findAddressByStreet(HabitatUniMToMBigInteger.class, "AAAAAAAAAAAAA");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        assertAddressForPerson1(adds.get(0));
    }

    @Override
    protected void update()
    {
        PersonnelUniMToMInt p1 = (PersonnelUniMToMInt) dao.findPerson(PersonnelUniMToMInt.class, 12345);
        Assert.assertNotNull(p1);
        p1.setPersonName("Saurabh");
        for (HabitatUniMToMBigInteger address : p1.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        dao.merge(p1);
        PersonnelUniMToMInt p1AfterMerge = (PersonnelUniMToMInt) dao.findPerson(PersonnelUniMToMInt.class, 12345);
        Assert.assertNotNull(p1AfterMerge);
        Assert.assertEquals("Saurabh", p1AfterMerge.getPersonName());
        for (HabitatUniMToMBigInteger address : p1AfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }

        PersonnelUniMToMInt p2 = (PersonnelUniMToMInt) dao.findPerson(PersonnelUniMToMInt.class, 12346);
        Assert.assertNotNull(p2);
        p2.setPersonName("Vijay");
        for (HabitatUniMToMBigInteger address : p2.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        dao.merge(p2);
        PersonnelUniMToMInt p2AfterMerge = (PersonnelUniMToMInt) dao.findPerson(PersonnelUniMToMInt.class, 12346);
        Assert.assertNotNull(p2AfterMerge);
        Assert.assertEquals("Vijay", p2AfterMerge.getPersonName());
        for (HabitatUniMToMBigInteger address : p2AfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }
    }

    @Override
    protected void remove()
    {
        dao.remove(12345, PersonnelUniMToMInt.class);
        PersonnelUniMToMInt person1AfterRemoval = (PersonnelUniMToMInt) dao
                .findPerson(PersonnelUniMToMInt.class, 12345);
        Assert.assertNull(person1AfterRemoval);

        dao.remove(12346, PersonnelUniMToMInt.class);
        PersonnelUniMToMInt person2AfterRemoval = (PersonnelUniMToMInt) dao
                .findPerson(PersonnelUniMToMInt.class, 12346);
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
//        tearDownInternal(ALL_PUs_UNDER_TEST);
    }

    /**
     * @param person1
     */
    private void assertPerson1(PersonnelUniMToMInt person1)
    {
        Assert.assertNotNull(person1);
        Assert.assertEquals(12345, person1.getPersonId());
        Assert.assertEquals("Amresh", person1.getPersonName());

        Set<HabitatUniMToMBigInteger> addresses1 = person1.getAddresses();
        Assert.assertNotNull(addresses1);
        Assert.assertFalse(addresses1.isEmpty());
        Assert.assertEquals(2, addresses1.size());
        HabitatUniMToMBigInteger address11 = (HabitatUniMToMBigInteger) addresses1.toArray()[0];
        assertAddressForPerson1(address11);
        HabitatUniMToMBigInteger address12 = (HabitatUniMToMBigInteger) addresses1.toArray()[1];
        assertAddressForPerson1(address12);
    }

    /**
     * @param person2
     */
    private void assertPerson2(PersonnelUniMToMInt person2)
    {
        Assert.assertNotNull(person2);

        Assert.assertEquals(12346, person2.getPersonId());
        Assert.assertEquals("Vivek", person2.getPersonName());

        Set<HabitatUniMToMBigInteger> addresses2 = person2.getAddresses();
        Assert.assertNotNull(addresses2);
        Assert.assertFalse(addresses2.isEmpty());
        Assert.assertEquals(2, addresses2.size());
        HabitatUniMToMBigInteger address21 = (HabitatUniMToMBigInteger) addresses2.toArray()[0];
        assertAddressForPerson2(address21);
        HabitatUniMToMBigInteger address22 = (HabitatUniMToMBigInteger) addresses2.toArray()[1];
        assertAddressForPerson2(address22);
    }

    /**
     * @param address11
     */
    private void assertAddressForPerson1(HabitatUniMToMBigInteger address)
    {
        Assert.assertNotNull(address);
        Assert.assertTrue(addressID1.equals(address.getAddressId()) || addressID2.equals(address.getAddressId()));
        Assert.assertTrue("AAAAAAAAAAAAA".equals(address.getStreet()) || "BBBBBBBBBBBBBBB".equals(address.getStreet()));
    }

    /**
     * @param address12
     */
    private void assertAddressForPerson2(HabitatUniMToMBigInteger address)
    {
        Assert.assertNotNull(address);

        Assert.assertTrue(addressID2.equals(address.getAddressId()) || addressID2.equals(address.getAddressId())
                || addressID3.equals(address.getAddressId()));
        Assert.assertTrue("CCCCCCCCCCC".equals(address.getStreet()) || "BBBBBBBBBBBBBBB".equals(address.getStreet()));

    }

    @Override
    protected void loadDataForPERSONNEL() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {

        KsDef ksDef = null;

        CfDef cfDef = new CfDef();
        cfDef.name = "PERSONNEL";
        cfDef.keyspace = "KunderaTests";
        // cfDef.column_type = "Super";

        cfDef.setComparator_type("UTF8Type");
        cfDef.setDefault_validation_class("IntegerType");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef);

        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("ADDRESS_ID".getBytes()), "IntegerType");
        columnDef1.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef1);

        // ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("PERSON_ID"
        // .getBytes()), "IntegerType");
        // cfDef.addToColumn_metadata(columnDef2);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaTests");
            CassandraCli.client.set_keyspace("KunderaTests");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            // CassandraCli.client.set_keyspace("KunderaTests");
            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("PERSONNEL"))
                {

                    CassandraCli.client.system_drop_column_family("PERSONNEL");

                }
            }
            CassandraCli.client.system_add_column_family(cfDef);

        }
        catch (NotFoundException e)
        {
            addKeyspace(ksDef, cfDefs);
        }

        CassandraCli.client.set_keyspace("KunderaTests");

        loadDataForPersonnelAddress();

    }

    @Override
    protected void loadDataForHABITAT() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef cfDef2 = new CfDef();
        cfDef2.name = "ADDRESS";
        cfDef2.keyspace = "KunderaTests";
        cfDef2.setDefault_validation_class("IntegerType");

        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        cfDef2.addToColumn_metadata(columnDef1);

        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("PERSON_ID".getBytes()), "IntegerType");
        columnDef2.index_type = IndexType.KEYS;
        cfDef2.addToColumn_metadata(columnDef2);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef2);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaTests");
            CassandraCli.client.set_keyspace("KunderaTests");
            List<CfDef> cfDefss = ksDef.getCf_defs();
            // CassandraCli.client.set_keyspace("KunderaTests");
            for (CfDef cfDef : cfDefss)
            {

                if (cfDef.getName().equalsIgnoreCase("ADDRESS"))
                {

                    CassandraCli.client.system_drop_column_family("ADDRESS");

                }
            }
            CassandraCli.client.system_add_column_family(cfDef2);
        }
        catch (NotFoundException e)
        {

            addKeyspace(ksDef, cfDefs);

        }
        CassandraCli.client.set_keyspace("KunderaTests");
    }

    static void loadDataForPersonnelAddress()
    {
        try
        {
            KsDef ksDef = CassandraCli.client.describe_keyspace("KunderaTests");
            CfDef cfDef2 = new CfDef();
            cfDef2.name = "PERSONNEL_ADDRESS";
            cfDef2.keyspace = "KunderaTests";

            List<CfDef> cfDefss = ksDef.getCf_defs();
            CassandraCli.client.set_keyspace("KunderaTests");
            for (CfDef cfDef : cfDefss)
            {

                if (cfDef.getName().equalsIgnoreCase("PERSONNEL_ADDRESS"))
                {

                    CassandraCli.client.system_drop_column_family("PERSONNEL_ADDRESS");

                }
            }
            CassandraCli.client.system_add_column_family(cfDef2);
        }
        catch (NotFoundException e)
        {
            e.printStackTrace();
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }
        catch (TException e)
        {
            e.printStackTrace();
        }
        catch (SchemaDisagreementException e)
        {
            e.printStackTrace();
        }

    }

}
