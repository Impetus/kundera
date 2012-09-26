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
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.HabitatBiMToMShort;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.PersonnelBiMToMInt;

public class MTMBiAssociationIntTest extends TwinAssociation

{
    public static final String[] ALL_PUs_UNDER_TEST = new String[] { "addCassandra", "addMongo" };

    private short addressID1 = Short.MAX_VALUE;

    private short addressID2 = Short.MIN_VALUE;

    private short addressID3 = (short) 3;

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

        if (AUTO_MANAGE_SCHEMA)
        {
            CassandraCli.initClient();
        }

        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelBiMToMInt.class);
        clazzz.add(HabitatBiMToMShort.class);
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
        setUpInternal();
    }

    @Test
    public void testCRUD()
    {
        // tryOperation(ALL_PUs_UNDER_TEST);
    }

    @Override
    protected void insert()
    {
        PersonnelBiMToMInt person1 = new PersonnelBiMToMInt();
        person1.setPersonId(1234);
        person1.setPersonName("Amresh");

        PersonnelBiMToMInt person2 = new PersonnelBiMToMInt();
        person2.setPersonId(1235);
        person2.setPersonName("Vivek");

        HabitatBiMToMShort address1 = new HabitatBiMToMShort();
        address1.setAddressId(addressID1);
        address1.setStreet("AAAAAAAAAAAAA");

        HabitatBiMToMShort address2 = new HabitatBiMToMShort();
        address2.setAddressId(addressID2);
        address2.setStreet("BBBBBBBBBBBBBBB");

        HabitatBiMToMShort address3 = new HabitatBiMToMShort();
        address3.setAddressId(addressID3);
        address3.setStreet("CCCCCCCCCCC");

        Set<HabitatBiMToMShort> person1Addresses = new HashSet<HabitatBiMToMShort>();
        Set<HabitatBiMToMShort> person2Addresses = new HashSet<HabitatBiMToMShort>();

        person1Addresses.add(address1);
        person1Addresses.add(address2);

        person2Addresses.add(address2);
        person2Addresses.add(address3);

        person1.setAddresses(person1Addresses);
        person2.setAddresses(person2Addresses);

        Set<PersonnelBiMToMInt> persons = new HashSet<PersonnelBiMToMInt>();
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
        PersonnelBiMToMInt person1 = (PersonnelBiMToMInt) dao.findPerson(PersonnelBiMToMInt.class, 1234);
        assertPerson1(person1);

        PersonnelBiMToMInt person2 = (PersonnelBiMToMInt) dao.findPerson(PersonnelBiMToMInt.class, 1235);
        assertPerson2(person2);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        // Find Person 1
        PersonnelBiMToMInt p1 = (PersonnelBiMToMInt) dao.findPersonByIdColumn(PersonnelBiMToMInt.class, 1234);
        assertPerson1(p1);

        // Find Person 2
        PersonnelBiMToMInt p2 = (PersonnelBiMToMInt) dao.findPersonByIdColumn(PersonnelBiMToMInt.class, 1235);
        assertPerson2(p2);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelBiMToMInt> persons = dao.findPersonByName(PersonnelBiMToMInt.class, "Amresh");
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
//        tearDownInternal(ALL_PUs_UNDER_TEST);
    }

    /**
     * @param person2
     */
    private void assertPerson2(PersonnelBiMToMInt person2)
    {
        Assert.assertNotNull(person2);

        Assert.assertEquals(1235, person2.getPersonId());
        Assert.assertEquals("Vivek", person2.getPersonName());

        Set<HabitatBiMToMShort> addresses2 = person2.getAddresses();
        Assert.assertNotNull(addresses2);
        Assert.assertFalse(addresses2.isEmpty());
        Assert.assertEquals(2, addresses2.size());
        HabitatBiMToMShort address21 = (HabitatBiMToMShort) addresses2.toArray()[0];
        Assert.assertNotNull(address21);
        HabitatBiMToMShort address22 = (HabitatBiMToMShort) addresses2.toArray()[1];
        Assert.assertNotNull(address22);
    }

    /**
     * @param person1
     */
    private void assertPerson1(PersonnelBiMToMInt person1)
    {
        Assert.assertNotNull(person1);
        Assert.assertEquals(1234, person1.getPersonId());
        Assert.assertEquals("Amresh", person1.getPersonName());

        Set<HabitatBiMToMShort> addresses1 = person1.getAddresses();
        Assert.assertNotNull(addresses1);
        Assert.assertFalse(addresses1.isEmpty());
        Assert.assertEquals(2, addresses1.size());
        HabitatBiMToMShort address11 = (HabitatBiMToMShort) addresses1.toArray()[0];
        Assert.assertNotNull(address11);
        HabitatBiMToMShort address12 = (HabitatBiMToMShort) addresses1.toArray()[1];
        Assert.assertNotNull(address12);
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
