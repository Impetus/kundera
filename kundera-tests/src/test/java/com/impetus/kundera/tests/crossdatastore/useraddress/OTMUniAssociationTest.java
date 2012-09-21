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
package com.impetus.kundera.tests.crossdatastore.useraddress;

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
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatUni1ToM;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelUni1ToM;

/**
 * @author vivek.mishra
 * 
 */
public class OTMUniAssociationTest extends TwinAssociation
{
    public static final String[] ALL_PUs_UNDER_TEST = new String[] { "rdbms", "addCassandra", /*
                                                                                               * "addHbase"
                                                                                               * ,
                                                                                               */"addMongo" };

    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {

        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelUni1ToM.class);
        clazzz.add(HabitatUni1ToM.class);
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
        tryOperation(ALL_PUs_UNDER_TEST);
    }

    @Override
    protected void insert()
    {
        // Save Person
        PersonnelUni1ToM personnel = new PersonnelUni1ToM();
        personnel.setPersonId("unionetomany_1");
        personnel.setPersonName("Amresh");

        Set<HabitatUni1ToM> addresses = new HashSet<HabitatUni1ToM>();
        HabitatUni1ToM address1 = new HabitatUni1ToM();
        address1.setAddressId("unionetomany_a");
        address1.setStreet("AAAAAAAAAAAAA");

        HabitatUni1ToM address2 = new HabitatUni1ToM();
        address2.setAddressId("unionetomany_b");
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
        PersonnelUni1ToM p = (PersonnelUni1ToM) dao.findPerson(PersonnelUni1ToM.class, "unionetomany_1");
        assertPerson(p);
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelUni1ToM p = (PersonnelUni1ToM) dao.findPersonByIdColumn(PersonnelUni1ToM.class, "unionetomany_1");
        assertPerson(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelUni1ToM> persons = dao.findPersonByName(PersonnelUni1ToM.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelUni1ToM person = persons.get(0);
        assertPerson(person);
    }

    @Override
    protected void findAddressByIdColumn()
    {
        HabitatUni1ToM a = (HabitatUni1ToM) dao.findAddressByIdColumn(HabitatUni1ToM.class, "unionetomany_a");
        Assert.assertNotNull(a);
        Assert.assertEquals("unionetomany_a", a.getAddressId());
        Assert.assertEquals("AAAAAAAAAAAAA", a.getStreet());
    }

    @Override
    protected void findAddressByStreet()
    {
        List<HabitatUni1ToM> adds = dao.findAddressByStreet(HabitatUni1ToM.class, "AAAAAAAAAAAAA");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        HabitatUni1ToM a = adds.get(0);
        Assert.assertNotNull(a);
        Assert.assertEquals("unionetomany_a", a.getAddressId());
        Assert.assertEquals("AAAAAAAAAAAAA", a.getStreet());
    }

    @Override
    protected void update()
    {
        try
        {
            PersonnelUni1ToM p = (PersonnelUni1ToM) dao.findPerson(PersonnelUni1ToM.class, "unionetomany_1");
            Assert.assertNotNull(p);
            p.setPersonName("Saurabh");

            for (HabitatUni1ToM address : p.getAddresses())
            {
                address.setStreet("Brand New Street");
            }
            dao.merge(p);
            assertPersonAfterUpdate();
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
        try
        {
            // PersonnelUni1ToM p = (PersonnelUni1ToM)
            // dao.findPerson(PersonnelUni1ToM.class, );
            dao.remove("unionetomany_1", PersonnelUni1ToM.class);
            PersonnelUni1ToM pAfterRemoval = (PersonnelUni1ToM) dao
                    .findPerson(PersonnelUni1ToM.class, "unionetomany_1");
            Assert.assertNull(pAfterRemoval);

        }
        catch (Exception e)
        {
            e.printStackTrace();
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
     * @param p
     */
    private void assertPerson(PersonnelUni1ToM p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("unionetomany_1", p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        Set<HabitatUni1ToM> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (HabitatUni1ToM address : adds)
        {
            Assert.assertNotNull(address.getStreet());
        }
    }

    /**
     * 
     */
    private void assertPersonAfterUpdate()
    {
        PersonnelUni1ToM pAfterMerge = (PersonnelUni1ToM) dao.findPerson(PersonnelUni1ToM.class, "unionetomany_1");
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());

        for (HabitatUni1ToM address : pAfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }
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
        cfDef.setDefault_validation_class("UTF8Type");
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
