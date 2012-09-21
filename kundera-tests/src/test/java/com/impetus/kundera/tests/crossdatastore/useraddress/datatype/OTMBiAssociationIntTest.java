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
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.HabitatBi1ToMDouble;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.PersonnelBi1ToMInt;

public class OTMBiAssociationIntTest extends TwinAssociation
{
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
        clazzz.add(PersonnelBi1ToMInt.class);
        clazzz.add(HabitatBi1ToMDouble.class);
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

    /**
     * Test insert.
     */
    @Test
    public void testCRUD()
    {
        tryOperation(ALL_PUs_UNDER_TEST);
    }

    @Override
    protected void insert()
    {
        PersonnelBi1ToMInt personnel = new PersonnelBi1ToMInt();
        personnel.setPersonId(12345);
        personnel.setPersonName("Amresh");

        Set<HabitatBi1ToMDouble> addresses = new HashSet<HabitatBi1ToMDouble>();
        HabitatBi1ToMDouble address1 = new HabitatBi1ToMDouble();
        address1.setAddressId(12345.1234);
        address1.setStreet("AAAAAAAAAAAAA");

        HabitatBi1ToMDouble address2 = new HabitatBi1ToMDouble();
        address2.setAddressId(12346.1234);
        address2.setStreet("BBBBBBBBBBB");

        addresses.add(address1);
        addresses.add(address2);
        personnel.setAddresses(addresses);
        dao.insert(personnel);
        col.add(personnel);
        col.add(address1);
        col.add(address2);

    }

    @Override
    protected void find()
    {
        // Find Person
        PersonnelBi1ToMInt p = (PersonnelBi1ToMInt) dao.findPerson(PersonnelBi1ToMInt.class, 12345);
        assertPerson(p);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelBi1ToMInt p = (PersonnelBi1ToMInt) dao.findPersonByIdColumn(PersonnelBi1ToMInt.class, 12345);
        assertPerson(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelBi1ToMInt> persons = dao.findPersonByName(PersonnelBi1ToMInt.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelBi1ToMInt person = persons.get(0);
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
        PersonnelBi1ToMInt p = (PersonnelBi1ToMInt) dao.findPerson(PersonnelBi1ToMInt.class, 12345);
        assertPerson(p);

        p.setPersonName("Saurabh");
        for (HabitatBi1ToMDouble address : p.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        dao.merge(p);
        PersonnelBi1ToMInt pAfterMerge = (PersonnelBi1ToMInt) dao.findPerson(PersonnelBi1ToMInt.class, 12345);

        assertPersonAfterUpdate(pAfterMerge);
    }

    @Override
    protected void remove()
    {
        // Find Person
        PersonnelBi1ToMInt p = (PersonnelBi1ToMInt) dao.findPerson(PersonnelBi1ToMInt.class, 12345);
        assertPersonAfterUpdate(p);

        dao.remove(12345, PersonnelBi1ToMInt.class);
        PersonnelBi1ToMInt pAfterRemoval = (PersonnelBi1ToMInt) dao.findPerson(PersonnelBi1ToMInt.class, 12345);
        Assert.assertNull(pAfterRemoval);
    }

    private void assertPerson(PersonnelBi1ToMInt p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(12345, p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        Set<HabitatBi1ToMDouble> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (HabitatBi1ToMDouble address : adds)
        {
            Assert.assertNotNull(address);
            PersonnelBi1ToMInt person = address.getPerson();
            Assert.assertNotNull(person);
            Assert.assertEquals(p.getPersonId(), person.getPersonId());
            Assert.assertEquals(p.getPersonName(), person.getPersonName());
            Assert.assertNotNull(person.getAddresses());
            Assert.assertFalse(person.getAddresses().isEmpty());
            Assert.assertEquals(2, person.getAddresses().size());
        }
    }

    private void assertPersonAfterUpdate(PersonnelBi1ToMInt p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(12345, p.getPersonId());
        Assert.assertEquals("Saurabh", p.getPersonName());

        Set<HabitatBi1ToMDouble> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (HabitatBi1ToMDouble address : adds)
        {
            Assert.assertNotNull(address);
            Assert.assertEquals("Brand New Street", address.getStreet());
            PersonnelBi1ToMInt person = address.getPerson();
            Assert.assertNotNull(person);
            Assert.assertEquals(p.getPersonId(), person.getPersonId());
            Assert.assertEquals(p.getPersonName(), person.getPersonName());
            Assert.assertNotNull(person.getAddresses());
            Assert.assertFalse(person.getAddresses().isEmpty());
            Assert.assertEquals(2, person.getAddresses().size());
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
//        tearDownInternal(ALL_PUs_UNDER_TEST);
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

    }

    @Override
    protected void loadDataForHABITAT() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef cfDef2 = new CfDef();
        cfDef2.name = "ADDRESS";
        cfDef2.keyspace = "KunderaTests";
        cfDef2.setDefault_validation_class("DoubleType");

        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        cfDef2.addToColumn_metadata(columnDef1);

        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("PERSON_ID".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        cfDef2.addToColumn_metadata(columnDef2);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef2);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaTests");
            CassandraCli.client.set_keyspace("KunderaTests");
            List<CfDef> cfDefss = ksDef.getCf_defs();
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
}
