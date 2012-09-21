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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.TwinAssociation;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.HabitatUni1To1FKInteger;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.PersonnelUni1To1FKInt;

/**
 * One to one association test for {@see} cassandra, mongodb,HBase and RDBMS
 * combination.
 * 
 * Script to create super column family
 * 
 * Note: As PersonnelUni1To1FK is holding embedded collection so to test as a
 * super col family. create super column family as given below: create column
 * family PERSONNEL with comparator=UTF8Type and
 * default_validation_class=UTF8Type and key_validation_class=UTF8Type and
 * column_type=Super;
 * 
 * 
 * Note: To create as column family PERSONNEL use as given below: create column
 * family PERSONNEL with comparator=UTF8Type and column_metadata=[{column_name:
 * PERSON_NAME, validation_class: UTF8Type, index_type: KEYS}, {column_name:
 * AGE, validation_class: IntegerType, index_type: KEYS}];
 * 
 * @author vivek.mishra
 */

public class OTOUniAssociationIntTest extends TwinAssociation
{
    public static final String[] ALL_PUs_UNDER_TEST = new String[] { "addCassandra", "addMongo" };

    private static final Integer ADDRESS_ID = new Integer(1234567);

    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            // HBaseCli cli = new HBaseCli();
            // cli.startCluster();
        }
        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelUni1To1FKInt.class);
        clazzz.add(HabitatUni1To1FKInteger.class);
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
     * Test CRUD
     */
    @Test
    public void testCRUD()
    {
        try
        {
            tryOperation(ALL_PUs_UNDER_TEST);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail(e.getMessage());

        }

    }

    protected void insert()
    {
        try
        {
            PersonnelUni1To1FKInt person = new PersonnelUni1To1FKInt();
            HabitatUni1To1FKInteger address = new HabitatUni1To1FKInteger();
            person.setPersonId(1234);
            person.setPersonName("Amresh");
            address.setAddressId(ADDRESS_ID);
            address.setStreet("123, New street");
            person.setAddress(address);
            dao.insert(person);
            col.add(person);
            col.add(address);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    protected void find()
    {
        // Find Person
        try
        {
            PersonnelUni1To1FKInt p = (PersonnelUni1To1FKInt) dao.findPerson(PersonnelUni1To1FKInt.class, 1234);
            assertPersonBeforeUpdate(p);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelUni1To1FKInt p = (PersonnelUni1To1FKInt) dao.findPersonByIdColumn(PersonnelUni1To1FKInt.class, 1234);
        assertPersonBeforeUpdate(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelUni1To1FKInt> persons = dao.findPersonByName(PersonnelUni1To1FKInt.class, "Amresh");
        assertPersons(persons);
    }

    @Override
    protected void findAddressByIdColumn()
    {
        HabitatUni1To1FKInteger a = (HabitatUni1To1FKInteger) dao.findAddressByIdColumn(HabitatUni1To1FKInteger.class,
                ADDRESS_ID);
        assertAddressBeforeUpdate(a);
    }

    @Override
    protected void findAddressByStreet()
    {
        List<HabitatUni1To1FKInteger> adds = dao.findAddressByStreet(HabitatUni1To1FKInteger.class, "123, New street");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        assertAddressBeforeUpdate(adds.get(0));
    }

    protected void update()
    {
        try
        {
            PersonnelUni1To1FKInt p = (PersonnelUni1To1FKInt) dao.findPerson(PersonnelUni1To1FKInt.class, 1234);
            Assert.assertNotNull(p);

            // dao.merge(p); //This merge operation should do nothing since
            // nothing has changed

            // p = (PersonnelUni1To1FK) dao.findPerson(PersonnelUni1To1FK.class,
            // 1234);
            p.setPersonName("Saurabh");
            p.getAddress().setStreet("Brand New Street");
            dao.merge(p);

            PersonnelUni1To1FKInt pAfterMerge = (PersonnelUni1To1FKInt) dao.findPerson(PersonnelUni1To1FKInt.class,
                    1234);
            assertPersonAfterUpdate(pAfterMerge);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail();
        }
    }

    protected void remove()
    {
        try
        {
            // PersonnelUni1To1FK p = (PersonnelUni1To1FK)
            // dao.findPerson(PersonnelUni1To1FK.class, 1234);
            dao.remove(1234, PersonnelUni1To1FKInt.class);

            PersonnelUni1To1FKInt pAfterRemoval = (PersonnelUni1To1FKInt) dao.findPerson(PersonnelUni1To1FKInt.class,
                    1234);
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
//        tearDownInternal(ALL_PUs_UNDER_TEST);

    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (RUN_IN_EMBEDDED_MODE)
        {
            // HBaseCli.stopCluster();
        }
    }

    @Override
    public void loadDataForPERSONNEL() throws InvalidRequestException, TException, SchemaDisagreementException
    {

        KsDef ksDef = null;

        CfDef cfDef = new CfDef();
        cfDef.name = "PERSONNEL";
        cfDef.keyspace = "KunderaTests";
        cfDef.setComparator_type("UTF8Type");
        cfDef.setDefault_validation_class("IntegerType");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.setIndex_type(IndexType.KEYS);
        cfDef.addToColumn_metadata(columnDef);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef);

        try
        {
            ksDef = com.impetus.kundera.tests.cli.CassandraCli.client.describe_keyspace("KunderaTests");
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
    public void loadDataForHABITAT() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef cfDef2 = new CfDef();
        cfDef2.name = "ADDRESS";
        cfDef2.keyspace = "KunderaTests";
        cfDef2.setDefault_validation_class("IntegerType");
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
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

    /**
     * @param p
     */
    private void assertPersonBeforeUpdate(PersonnelUni1To1FKInt p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1234, p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        assertAddressBeforeUpdate(p.getAddress());
    }

    /**
     * @param p
     */
    private void assertAddressBeforeUpdate(HabitatUni1To1FKInteger add)
    {
        Assert.assertNotNull(add);
        Assert.assertNotNull(add.getAddressId());
        Assert.assertEquals(ADDRESS_ID, add.getAddressId());
        Assert.assertEquals("123, New street", add.getStreet());
    }

    /**
     * @param pAfterMerge
     */
    private void assertPersonAfterUpdate(PersonnelUni1To1FKInt pAfterMerge)
    {
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());
        HabitatUni1To1FKInteger addressAfterMerge = pAfterMerge.getAddress();
        Assert.assertNotNull(addressAfterMerge);
        Assert.assertEquals("Brand New Street", addressAfterMerge.getStreet());
    }

    /**
     * @param persons
     */
    private void assertPersons(List<PersonnelUni1To1FKInt> persons)
    {
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelUni1To1FKInt person = persons.get(0);
        assertPersonBeforeUpdate(person);
    }

}