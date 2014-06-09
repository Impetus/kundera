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
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatUniMTo1;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelUniMTo1;

/**
 * @author vivek.mishra
 * 
 */
public class MTOUniAssociationTest extends TwinAssociation
{
    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {

        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelUniMTo1.class);
        clazzz.add(HabitatUniMTo1.class);
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
        setUpInternal("HabitatUniMTo1", "PersonnelUniMTo1");
    }

    /**
     * Test CRUD.
     */
    @Test
    public void testCRUD()
    {
        tryOperation(ALL_PUs_UNDER_TEST);
    }

    @Override
    protected void insert()
    {
        PersonnelUniMTo1 person1 = new PersonnelUniMTo1();
        person1.setPersonId("unimanytoone_1");
        person1.setPersonName("Amresh");

        PersonnelUniMTo1 person2 = new PersonnelUniMTo1();
        person2.setPersonId("unimanytoone_2");
        person2.setPersonName("Vivek");

        HabitatUniMTo1 address = new HabitatUniMTo1();
        address.setAddressId("unimanytoone_a");
        address.setStreet("AAAAAAAAAAAAA");

        person1.setAddress(address);
        person2.setAddress(address);

        Set<PersonnelUniMTo1> persons = new HashSet<PersonnelUniMTo1>();
        persons.add(person1);
        persons.add(person2);

        dao.savePersons(persons);

        col.add(person1);
        col.add(person2);
        col.add(address);
    }

    @Override
    protected void find()
    {
        // Find Person 1
        PersonnelUniMTo1 p1 = (PersonnelUniMTo1) dao.findPerson(PersonnelUniMTo1.class, "unimanytoone_1");
        assertPerson1(p1);

        // Find Person 2
        PersonnelUniMTo1 p2 = (PersonnelUniMTo1) dao.findPerson(PersonnelUniMTo1.class, "unimanytoone_2");
        assertPerson2(p2);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelUniMTo1 p = (PersonnelUniMTo1) dao.findPersonByIdColumn(PersonnelUniMTo1.class, "unimanytoone_1");
        assertPerson1(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelUniMTo1> persons = dao.findPersonByName(PersonnelUniMTo1.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelUniMTo1 person = persons.get(0);
        assertPerson1(person);
    }

    @Override
    protected void findAddressByIdColumn()
    {
        HabitatUniMTo1 a = (HabitatUniMTo1) dao.findAddressByIdColumn(HabitatUniMTo1.class, "unimanytoone_a");
        assertAddress(a);
    }

    @Override
    protected void findAddressByStreet()
    {
        List<HabitatUniMTo1> adds = dao.findAddressByStreet(HabitatUniMTo1.class, "AAAAAAAAAAAAA");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        assertAddress(adds.get(0));
    }

    @Override
    protected void update()
    {
        // Find Person 1
        PersonnelUniMTo1 p1 = (PersonnelUniMTo1) dao.findPerson(PersonnelUniMTo1.class, "unimanytoone_1");
        Assert.assertNotNull(p1);
        p1.setPersonName("Saurabh");
        p1.getAddress().setStreet("Brand New Street");
        dao.merge(p1);
        PersonnelUniMTo1 p1AfterMerge = (PersonnelUniMTo1) dao.findPerson(PersonnelUniMTo1.class, "unimanytoone_1");
        Assert.assertNotNull(p1AfterMerge);
        Assert.assertEquals("Saurabh", p1AfterMerge.getPersonName());
        Assert.assertEquals("Brand New Street", p1AfterMerge.getAddress().getStreet());

        // Find Person 2
        PersonnelUniMTo1 p2 = (PersonnelUniMTo1) dao.findPerson(PersonnelUniMTo1.class, "unimanytoone_2");
        Assert.assertNotNull(p2);
        Assert.assertNotNull(p2.getAddress());
        Assert.assertNotNull(p2.getAddress().getAddressId());
        p2.setPersonName("Prateek");
        dao.merge(p2);
        PersonnelUniMTo1 p2AfterMerge = (PersonnelUniMTo1) dao.findPerson(PersonnelUniMTo1.class, "unimanytoone_2");
        Assert.assertNotNull(p2AfterMerge);
        Assert.assertEquals("Prateek", p2AfterMerge.getPersonName());
        Assert.assertEquals("Brand New Street", p2AfterMerge.getAddress().getStreet());
    }

    @Override
    protected void remove()
    {
        // Remove Person 1
        dao.remove("unimanytoone_1", PersonnelUniMTo1.class);
        PersonnelUniMTo1 p1AfterRemoval = (PersonnelUniMTo1) dao.findPerson(PersonnelUniMTo1.class, "unimanytoone_1");
        Assert.assertNull(p1AfterRemoval);

        // Remove Person 2
        dao.remove("unimanytoone_2", PersonnelUniMTo1.class);
        PersonnelUniMTo1 p2AfterRemoval = (PersonnelUniMTo1) dao.findPerson(PersonnelUniMTo1.class, "unimanytoone_2");
        Assert.assertNull(p2AfterRemoval);
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
     * @param p2
     */
    private void assertPerson2(PersonnelUniMTo1 p2)
    {
        Assert.assertNotNull(p2);
        Assert.assertEquals("unimanytoone_2", p2.getPersonId());
        Assert.assertEquals("Vivek", p2.getPersonName());

        HabitatUniMTo1 add2 = p2.getAddress();
        assertAddress(add2);
    }

    /**
     * @param p1
     */
    private void assertPerson1(PersonnelUniMTo1 p1)
    {
        Assert.assertNotNull(p1);
        Assert.assertEquals("unimanytoone_1", p1.getPersonId());
        Assert.assertEquals("Amresh", p1.getPersonName());

        HabitatUniMTo1 add = p1.getAddress();
        assertAddress(add);
    }

    /**
     * @param add2
     */
    private void assertAddress(HabitatUniMTo1 add2)
    {
        Assert.assertNotNull(add2);

        Assert.assertEquals("unimanytoone_a", add2.getAddressId());
        Assert.assertEquals("AAAAAAAAAAAAA", add2.getStreet());
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
                    ByteBuffer.wrap("drop table \"PersonnelUniMTo1\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"PersonnelUniMTo1\" ( \"PERSON_ID\" text PRIMARY KEY,  \"PERSON_NAME\" text, \"ADDRESS_ID\" text)",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelUniMTo1\" ( \"PERSON_NAME\")", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelUniMTo1\" ( \"ADDRESS_ID\")", keyspaceName);
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
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("drop table \"HabitatUniMTo1\"".getBytes("UTF-8")),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli.executeCqlQuery(
                "create table \"HabitatUniMTo1\" ( \"ADDRESS_ID\" text PRIMARY KEY,  \"STREET\" text)", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatUniMTo1\" ( \"STREET\")", keyspaceName);
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
            // cli.update("CREATE TABLE KUNDERATESTS.PersonnelUniMTo1 (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(150))");
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelUniMTo1 (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(150), ADDRESS_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PersonnelUniMTo1");
            // cli.update("DROP TABLE KUNDERATESTS.PersonnelUniMTo1");
            // cli.update("CREATE TABLE KUNDERATESTS.PersonnelUniMTo1 (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(150))");
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
            // cli.update("CREATE TABLE KUNDERATESTS.HabitatUniMTo1 (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256))");
            cli.update("CREATE TABLE KUNDERATESTS.HabitatUniMTo1 (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256),PERSON_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.HabitatUniMTo1");
            // cli.update("DROP TABLE KUNDERATESTS.HabitatUniMTo1");
            // cli.update("CREATE TABLE KUNDERATESTS.HabitatUniMTo1 (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256))");
        }
    }

}
