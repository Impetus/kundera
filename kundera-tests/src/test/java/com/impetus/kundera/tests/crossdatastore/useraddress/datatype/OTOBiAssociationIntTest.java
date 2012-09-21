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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.HabitatBi1To1FKBigDecimal;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.PersonnelBi1To1FKInt;

public class OTOBiAssociationIntTest extends TwinAssociation
{
    public static final String[] ALL_PUs_UNDER_TEST = new String[] { "addCassandra", "addMongo" };

    private static final BigDecimal ADDRESS_ID = new BigDecimal("123456");

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
        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelBi1To1FKInt.class);
        clazzz.add(HabitatBi1To1FKBigDecimal.class);
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
     * Test CRUD
     */
    @Test
    public void testCRUD()
    {
        tryOperation(ALL_PUs_UNDER_TEST);
    }

    @Override
    protected void insert()
    {
        PersonnelBi1To1FKInt person = new PersonnelBi1To1FKInt();
        person.setPersonId(1234);
        person.setPersonName("Amresh");

        HabitatBi1To1FKBigDecimal address = new HabitatBi1To1FKBigDecimal();
        address.setAddressId(ADDRESS_ID);
        address.setStreet("123, New street");
        person.setAddress(address);
        address.setPerson(person);

        dao.insert(person);
        col.add(person);
        col.add(address);

    }

    @Override
    protected void find()
    {
        // Find Person
        PersonnelBi1To1FKInt p = (PersonnelBi1To1FKInt) dao.findPerson(PersonnelBi1To1FKInt.class, 1234);
        assertPersonnel(p);
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelBi1To1FKInt p = (PersonnelBi1To1FKInt) dao.findPersonByIdColumn(PersonnelBi1To1FKInt.class, 1234);
        assertPersonnel(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelBi1To1FKInt> persons = dao.findPersonByName(PersonnelBi1To1FKInt.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        assertPersonnel(persons.get(0));
    }

    @Override
    protected void findAddressByIdColumn()
    {
        /*
         * HabitatBi1To1FK a = (HabitatBi1To1FK)
         * dao.findAddressByIdColumn(HabitatBi1To1FK.class, ADDRESS_ID);
         * assertAddress(a);
         */
    }

    @Override
    protected void findAddressByStreet()
    {
        /*
         * List<HabitatBi1To1FK> adds =
         * dao.findAddressByStreet(HabitatBi1To1FK.class, "123, New street");
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
            PersonnelBi1To1FKInt p = (PersonnelBi1To1FKInt) dao.findPerson(PersonnelBi1To1FKInt.class, 1234);
            assertPersonnel(p);

            dao.merge(p); // This merge operation should do nothing since
                          // nothing has changed

            p = (PersonnelBi1To1FKInt) dao.findPerson(PersonnelBi1To1FKInt.class, 1234);
            assertPersonnel(p);

            p.setPersonName("Saurabh");
            p.getAddress().setStreet("Brand New Street");
            dao.merge(p);

            PersonnelBi1To1FKInt pAfterMerge = (PersonnelBi1To1FKInt) dao.findPerson(PersonnelBi1To1FKInt.class, 1234);
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
        PersonnelBi1To1FKInt p = (PersonnelBi1To1FKInt) dao.findPerson(PersonnelBi1To1FKInt.class, 1234);
        assertPersonnelAfterUpdate(p);

        dao.remove(1234, PersonnelBi1To1FKInt.class);

        PersonnelBi1To1FKInt pAfterRemoval = (PersonnelBi1To1FKInt) dao.findPerson(PersonnelBi1To1FKInt.class, 1234);
        Assert.assertNull(pAfterRemoval);
    }

    private void assertPersonnel(PersonnelBi1To1FKInt p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1234, p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        HabitatBi1To1FKBigDecimal address = p.getAddress();
        assertAddress(address);
    }

    /**
     * @param address
     */
    private void assertAddress(HabitatBi1To1FKBigDecimal address)
    {
        Assert.assertNotNull(address);
        Assert.assertEquals(ADDRESS_ID, address.getAddressId());
        Assert.assertEquals("123, New street", address.getStreet());

        PersonnelBi1To1FKInt pp = address.getPerson();
        Assert.assertNotNull(pp);
        Assert.assertEquals(1234, pp.getPersonId());
        Assert.assertEquals("Amresh", pp.getPersonName());
    }

    private void assertPersonnelAfterUpdate(PersonnelBi1To1FKInt pAfterMerge)
    {
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());
        HabitatBi1To1FKBigDecimal addressAfterMerge = pAfterMerge.getAddress();
        Assert.assertNotNull(addressAfterMerge);
        Assert.assertEquals("Brand New Street", addressAfterMerge.getStreet());

        PersonnelBi1To1FKInt pp = addressAfterMerge.getPerson();
        Assert.assertNotNull(pp);
        Assert.assertEquals(1234, pp.getPersonId());
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

        ColumnDef columnDefPersonName = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDefPersonName.index_type = IndexType.KEYS;

        ColumnDef columnDefAddressId = new ColumnDef(ByteBuffer.wrap("ADDRESS_ID".getBytes()), "UTF8Type");
        columnDefAddressId.index_type = IndexType.KEYS;

        cfDef.addToColumn_metadata(columnDefPersonName);
        cfDef.addToColumn_metadata(columnDefAddressId);

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
        cfDef2.setDefault_validation_class("DecimalType");

        ColumnDef columnDefStreet = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
        columnDefStreet.index_type = IndexType.KEYS;

        cfDef2.addToColumn_metadata(columnDefStreet);

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
}
