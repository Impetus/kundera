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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
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

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatUniMToM;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelUniMToM;

/**
 * @author vivek.mishra
 * 
 */
public class MTMUniAssociationTest extends TwinAssociation
{
    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {
        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelUniMToM.class);
        clazzz.add(HabitatUniMToM.class);
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
        setUpInternal("HabitatUniMToM", "PersonnelUniMToM", "PERSONNEL_ADDRESS");
    }

    /**
     * Test insert.
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
            Assert.fail("Failed, Caused by:" + e.getMessage());
        }
    }

    @Override
    protected void insert()
    {
        PersonnelUniMToM person1 = new PersonnelUniMToM();
        person1.setPersonId("unimanytomany_1");
        person1.setPersonName("Amresh");

        PersonnelUniMToM person2 = new PersonnelUniMToM();
        person2.setPersonId("unimanytomany_2");
        person2.setPersonName("Vivek");

        HabitatUniMToM address1 = new HabitatUniMToM();
        address1.setAddressId("unimanytomany_a");
        address1.setStreet("AAAAAAAAAAAAA");

        HabitatUniMToM address2 = new HabitatUniMToM();
        address2.setAddressId("unimanytomany_b");
        address2.setStreet("BBBBBBBBBBBBBBB");

        HabitatUniMToM address3 = new HabitatUniMToM();
        address3.setAddressId("unimanytomany_c");
        address3.setStreet("CCCCCCCCCCC");

        Set<HabitatUniMToM> person1Addresses = new HashSet<HabitatUniMToM>();
        Set<HabitatUniMToM> person2Addresses = new HashSet<HabitatUniMToM>();

        person1Addresses.add(address1);
        person1Addresses.add(address2);

        person2Addresses.add(address2);
        person2Addresses.add(address3);

        person1.setAddresses(person1Addresses);
        person2.setAddresses(person2Addresses);

        Set<PersonnelUniMToM> persons = new HashSet<PersonnelUniMToM>();
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

        PersonnelUniMToM person1 = (PersonnelUniMToM) dao.findPerson(PersonnelUniMToM.class, "unimanytomany_1");
        assertPerson1(person1);

        PersonnelUniMToM person2 = (PersonnelUniMToM) dao.findPerson(PersonnelUniMToM.class, "unimanytomany_2");
        assertPerson2(person2);
    }

    @Override
    protected void findPersonByIdColumn()
    {
        // Find Person 1
        PersonnelUniMToM p1 = (PersonnelUniMToM) dao.findPersonByIdColumn(PersonnelUniMToM.class, "unimanytomany_1");
        assertPerson1(p1);

        // Find Person 2
        PersonnelUniMToM p2 = (PersonnelUniMToM) dao.findPersonByIdColumn(PersonnelUniMToM.class, "unimanytomany_2");
        assertPerson2(p2);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelUniMToM> persons = dao.findPersonByName(PersonnelUniMToM.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        assertPerson1(persons.get(0));
    }

    @Override
    protected void findAddressByIdColumn()
    {
        HabitatUniMToM a = (HabitatUniMToM) dao.findAddressByIdColumn(HabitatUniMToM.class, "unimanytomany_a");
        assertAddressForPerson1(a);
    }

    @Override
    protected void findAddressByStreet()
    {
        List<HabitatUniMToM> adds = dao.findAddressByStreet(HabitatUniMToM.class, "AAAAAAAAAAAAA");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        assertAddressForPerson1(adds.get(0));
    }

    @Override
    protected void update()
    {
        PersonnelUniMToM p1 = (PersonnelUniMToM) dao.findPerson(PersonnelUniMToM.class, "unimanytomany_1");
        Assert.assertNotNull(p1);
        Assert.assertEquals(2, p1.getAddresses().size());
        for (HabitatUniMToM address : p1.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        p1.setPersonName("Saurabh");
        dao.merge(p1);
        PersonnelUniMToM p1AfterMerge = (PersonnelUniMToM) dao.findPerson(PersonnelUniMToM.class, "unimanytomany_1");
        Assert.assertNotNull(p1AfterMerge);
        Assert.assertEquals("Saurabh", p1AfterMerge.getPersonName());
        Assert.assertEquals(2, p1AfterMerge.getAddresses().size());
        for (HabitatUniMToM address : p1AfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }

        PersonnelUniMToM p2 = (PersonnelUniMToM) dao.findPerson(PersonnelUniMToM.class, "unimanytomany_2");
        Assert.assertNotNull(p2);
        Assert.assertEquals(2, p2.getAddresses().size());
        p2.setPersonName("Vijay");
        for (HabitatUniMToM address : p2.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        dao.merge(p2);
        PersonnelUniMToM p2AfterMerge = (PersonnelUniMToM) dao.findPerson(PersonnelUniMToM.class, "unimanytomany_2");
        Assert.assertNotNull(p2AfterMerge);
        Assert.assertEquals("Vijay", p2AfterMerge.getPersonName());
        Assert.assertEquals(2, p2AfterMerge.getAddresses().size());
        for (HabitatUniMToM address : p2AfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }
    }

    @Override
    protected void remove()
    {
        dao.remove("unimanytomany_1", PersonnelUniMToM.class);
        PersonnelUniMToM person1AfterRemoval = (PersonnelUniMToM) dao.findPerson(PersonnelUniMToM.class,
                "unimanytomany_1");
        Assert.assertNull(person1AfterRemoval);

        dao.remove("unimanytomany_2", PersonnelUniMToM.class);
        PersonnelUniMToM person2AfterRemoval = (PersonnelUniMToM) dao.findPerson(PersonnelUniMToM.class,
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
        // shutDownRdbmsServer();
        // tearDownInternal(ALL_PUs_UNDER_TEST);
    }

    /**
     * @param person1
     */
    private void assertPerson1(PersonnelUniMToM person1)
    {
        Assert.assertNotNull(person1);
        Assert.assertEquals("unimanytomany_1", person1.getPersonId());
        Assert.assertEquals("Amresh", person1.getPersonName());

        Set<HabitatUniMToM> addresses1 = person1.getAddresses();
        Assert.assertNotNull(addresses1);
        Assert.assertFalse(addresses1.isEmpty());
        Assert.assertEquals(2, addresses1.size());
        HabitatUniMToM address11 = (HabitatUniMToM) addresses1.toArray()[0];
        assertAddressForPerson1(address11);
        HabitatUniMToM address12 = (HabitatUniMToM) addresses1.toArray()[1];
        assertAddressForPerson1(address12);
    }

    /**
     * @param person2
     */
    private void assertPerson2(PersonnelUniMToM person2)
    {
        Assert.assertNotNull(person2);

        Assert.assertEquals("unimanytomany_2", person2.getPersonId());
        Assert.assertEquals("Vivek", person2.getPersonName());

        Set<HabitatUniMToM> addresses2 = person2.getAddresses();
        Assert.assertNotNull(addresses2);
        Assert.assertFalse(addresses2.isEmpty());
        Assert.assertEquals(2, addresses2.size());
        HabitatUniMToM address21 = (HabitatUniMToM) addresses2.toArray()[0];
        assertAddressForPerson2(address21);
        HabitatUniMToM address22 = (HabitatUniMToM) addresses2.toArray()[1];
        assertAddressForPerson2(address22);
    }

    /**
     * @param address11
     */
    private void assertAddressForPerson1(HabitatUniMToM address)
    {
        Assert.assertNotNull(address);
        Assert.assertTrue("unimanytomany_a".equals(address.getAddressId())
                || "unimanytomany_b".equals(address.getAddressId()));
        Assert.assertTrue("AAAAAAAAAAAAA".equals(address.getStreet()) || "BBBBBBBBBBBBBBB".equals(address.getStreet()));
    }

    /**
     * @param address12
     */
    private void assertAddressForPerson2(HabitatUniMToM address)
    {
        Assert.assertNotNull(address);

        Assert.assertTrue("unimanytomany_b".equals(address.getAddressId())
                || "unimanytomany_b".equals(address.getAddressId()) || "unimanytomany_c".equals(address.getAddressId()));
        Assert.assertTrue("CCCCCCCCCCC".equals(address.getStreet()) || "BBBBBBBBBBBBBBB".equals(address.getStreet()));

    }

    @Override
    protected void loadDataForPERSONNEL() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        String keyspaceName = "KunderaTests";
        CassandraCli.createKeySpace(keyspaceName);

        CassandraCli.client.set_keyspace(keyspaceName);
        try
        {
            CassandraCli.client.execute_cql3_query(
                    ByteBuffer.wrap("drop table \"PersonnelUniMToM\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"PersonnelUniMToM\" ( \"PERSON_ID\" text PRIMARY KEY,  \"PERSON_NAME\" text, \"ADDRESS_ID\" text)",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelUniMToM\" ( \"PERSON_NAME\")", keyspaceName);

        try
        {
            CassandraCli.client.execute_cql3_query(
                    ByteBuffer.wrap("drop table \"PERSONNEL_ADDRESS\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"PERSONNEL_ADDRESS\" ( \"PERSON_ID\" text,  \"key\" text PRIMARY KEY, \"ADDRESS_ID\" text)",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PERSONNEL_ADDRESS\" ( \"PERSON_ID\")", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PERSONNEL_ADDRESS\" ( \"ADDRESS_ID\")", keyspaceName);
    }

    @Override
    protected void loadDataForHABITAT() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        String keyspaceName = "KunderaTests";
        CassandraCli.createKeySpace(keyspaceName);

        CassandraCli.client.set_keyspace(keyspaceName);
        try
        {
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("drop table \"HabitatUniMToM\"".getBytes("UTF-8")),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli.executeCqlQuery(
                "create table \"HabitatUniMToM\" ( \"ADDRESS_ID\" text PRIMARY KEY,  \"STREET\" text)", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatUniMToM\" ( \"STREET\")", keyspaceName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.tests.crossdatastore.useraddress.AssociationBase#
     * createSchemaForPERSONNEL()
     */
    @Override
    protected void createSchemaForPERSONNEL() throws SQLException
    {
        try
        {
            // cli.update("CREATE TABLE KUNDERATESTS.PersonnelUniMToM (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256))");
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelUniMToM (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(150), ADDRESS_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PersonnelUniMToM");
            // cli.update("DROP TABLE KUNDERATESTS.PersonnelUniMToM");
            // cli.update("CREATE TABLE KUNDERATESTS.PersonnelUniMToM (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256))");
        }
        try
        {
            cli.update("CREATE TABLE KUNDERATESTS.PERSONNEL_ADDRESS (PERSON_ID VARCHAR(150) , ADDRESS_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PERSONNEL_ADDRESS");
            // cli.update("DROP TABLE KUNDERATESTS.PERSONNEL_ADDRESS");
            // cli.update("CREATE TABLE KUNDERATESTS.PERSONNEL_ADDRESS (PERSON_ID VARCHAR(150) , ADDRESS_ID VARCHAR(150))");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.tests.crossdatastore.useraddress.AssociationBase#
     * createSchemaForHABITAT()
     */
    @Override
    protected void createSchemaForHABITAT() throws SQLException
    {
        try
        {
            // cli.update("CREATE TABLE KUNDERATESTS.HabitatUniMToM (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256))");
            cli.update("CREATE TABLE KUNDERATESTS.HabitatUniMToM (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256),PERSON_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.HabitatUniMToM");
            // cli.update("DROP TABLE KUNDERATESTS.HabitatUniMToM");
            // cli.update("CREATE TABLE KUNDERATESTS.HabitatUniMToM (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256))");
        }
    }

}
