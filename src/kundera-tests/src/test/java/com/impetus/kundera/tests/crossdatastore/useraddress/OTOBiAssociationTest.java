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
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatBi1To1FK;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelBi1To1FK;

public class OTOBiAssociationTest extends TwinAssociation
{
    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {
        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelBi1To1FK.class);
        clazzz.add(HabitatBi1To1FK.class);
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

        setUpInternal("HabitatBi1To1FK","PersonnelBi1To1FK");
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
        PersonnelBi1To1FK person = new PersonnelBi1To1FK();
        person.setPersonId("bionetoonefk_1");
        person.setPersonName("Amresh");

        HabitatBi1To1FK address = new HabitatBi1To1FK();
        address.setAddressId("bionetoonefk_a");
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
        PersonnelBi1To1FK p = (PersonnelBi1To1FK) dao.findPerson(PersonnelBi1To1FK.class, "bionetoonefk_1");
        assertPersonnel(p);       
 
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelBi1To1FK p = (PersonnelBi1To1FK) dao.findPersonByIdColumn(PersonnelBi1To1FK.class, "bionetoonefk_1");
        assertPersonnel(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelBi1To1FK> persons = dao.findPersonByName(PersonnelBi1To1FK.class, "Amresh");
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
         * dao.findAddressByIdColumn(HabitatBi1To1FK.class, "bionetoonefk_a");
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
            PersonnelBi1To1FK p = (PersonnelBi1To1FK) dao.findPerson(PersonnelBi1To1FK.class, "bionetoonefk_1");
            assertPersonnel(p);

            dao.merge(p); // This merge operation should do nothing since
                          // nothing has changed

            p = (PersonnelBi1To1FK) dao.findPerson(PersonnelBi1To1FK.class, "bionetoonefk_1");
            assertPersonnel(p);

            p.setPersonName("Saurabh");
            p.getAddress().setStreet("Brand New Street");
            dao.merge(p);

            PersonnelBi1To1FK pAfterMerge = (PersonnelBi1To1FK) dao.findPerson(PersonnelBi1To1FK.class,
                    "bionetoonefk_1");
            assertPersonnelAfterUpdate(pAfterMerge);

        }
        catch (Exception e)
        {
            
            Assert.fail();
        }
    }

    @Override
    protected void remove()
    {
        PersonnelBi1To1FK p = (PersonnelBi1To1FK) dao.findPerson(PersonnelBi1To1FK.class, "bionetoonefk_1");
        assertPersonnelAfterUpdate(p);

        dao.remove("bionetoonefk_1", PersonnelBi1To1FK.class);

        PersonnelBi1To1FK pAfterRemoval = (PersonnelBi1To1FK) dao.findPerson(PersonnelBi1To1FK.class, "bionetoonefk_1");
        Assert.assertNull(pAfterRemoval);
    }

    private void assertPersonnel(PersonnelBi1To1FK p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("bionetoonefk_1", p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        HabitatBi1To1FK address = p.getAddress();       
        assertAddress(address);
    }

    /**
     * @param address
     */
    private void assertAddress(HabitatBi1To1FK address)
    {
        Assert.assertNotNull(address);
        Assert.assertEquals("bionetoonefk_a", address.getAddressId());
        Assert.assertEquals("123, New street", address.getStreet());

        PersonnelBi1To1FK pp = address.getPerson();
        Assert.assertNotNull(pp);
        Assert.assertEquals("bionetoonefk_1", pp.getPersonId());
        Assert.assertEquals("Amresh", pp.getPersonName());
    }

    private void assertPersonnelAfterUpdate(PersonnelBi1To1FK pAfterMerge)
    {
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());
        HabitatBi1To1FK addressAfterMerge = pAfterMerge.getAddress();
        Assert.assertNotNull(addressAfterMerge);
        Assert.assertEquals("Brand New Street", addressAfterMerge.getStreet());

        PersonnelBi1To1FK pp = addressAfterMerge.getPerson();
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
       // tearDownInternal(ALL_PUs_UNDER_TEST);
       // shutDownRdbmsServer();
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
                    ByteBuffer.wrap("drop table \"PersonnelBi1To1FK\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"PersonnelBi1To1FK\" ( \"PERSON_ID\" text PRIMARY KEY,  \"PERSON_NAME\" text, \"ADDRESS_ID\" text)",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelBi1To1FK\" ( \"PERSON_NAME\")", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelBi1To1FK\" ( \"ADDRESS_ID\")", keyspaceName);
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
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("drop table \"HabitatBi1To1FK\"".getBytes("UTF-8")),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli.executeCqlQuery(
                "create table \"HabitatBi1To1FK\" ( \"ADDRESS_ID\" text PRIMARY KEY,  \"STREET\" text)", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatBi1To1FK\" ( \"STREET\")", keyspaceName);
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
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelBi1To1FK (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PersonnelBi1To1FK");
//            cli.update("DROP TABLE KUNDERATESTS.PersonnelBi1To1FK");
//            cli.update("CREATE TABLE KUNDERATESTS.PersonnelBi1To1FK (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(150))");
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
          //  cli.update("CREATE TABLE KUNDERATESTS.HabitatBi1To1FK (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256))");
            cli.update("CREATE TABLE KUNDERATESTS.HabitatBi1To1FK (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(250),PERSON_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.HabitatBi1To1FK");
//            cli.update("DROP TABLE KUNDERATESTS.ADDRESS");
//            cli.update("CREATE TABLE KUNDERATESTS.HabitatBi1To1FK (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256))");
        }
    }
}
