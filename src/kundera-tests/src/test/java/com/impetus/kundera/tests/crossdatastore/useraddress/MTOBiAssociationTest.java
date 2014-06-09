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
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatBiMTo1;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelBiMTo1;

public class MTOBiAssociationTest extends TwinAssociation
{
    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {

        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelBiMTo1.class);
        clazzz.add(HabitatBiMTo1.class);
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
        setUpInternal("HabitatBiMTo1","PersonnelBiMTo1");
    }

    @Test
    public void testCRUD()
    {
        tryOperation(ALL_PUs_UNDER_TEST);
    }

    @Override
    protected void insert()
    {
        PersonnelBiMTo1 person1 = new PersonnelBiMTo1();
        person1.setPersonId("bimanytoone_1");
        person1.setPersonName("Amresh");

        PersonnelBiMTo1 person2 = new PersonnelBiMTo1();
        person2.setPersonId("bimanytoone_2");
        person2.setPersonName("Vivek");

        HabitatBiMTo1 address = new HabitatBiMTo1();
        address.setAddressId("bimanytoone_b");
        address.setStreet("AAAAAAAAAAAAA");

        person1.setAddress(address);
        person2.setAddress(address);

        Set<PersonnelBiMTo1> persons = new HashSet<PersonnelBiMTo1>();
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
        PersonnelBiMTo1 p1 = (PersonnelBiMTo1) dao.findPerson(PersonnelBiMTo1.class, "bimanytoone_1");
        assertPerson1(p1);

        // Find Person 2
        PersonnelBiMTo1 p2 = (PersonnelBiMTo1) dao.findPerson(PersonnelBiMTo1.class, "bimanytoone_2");
        assertPerson2(p2);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelBiMTo1 p = (PersonnelBiMTo1) dao.findPersonByIdColumn(PersonnelBiMTo1.class, "bimanytoone_1");
        assertPerson1(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelBiMTo1> persons = dao.findPersonByName(PersonnelBiMTo1.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelBiMTo1 person = persons.get(0);
        assertPerson1(person);
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
     * @param p2
     */
    private void assertPerson2(PersonnelBiMTo1 p2)
    {
        Assert.assertNotNull(p2);
        Assert.assertEquals("bimanytoone_2", p2.getPersonId());
        Assert.assertEquals("Vivek", p2.getPersonName());

        HabitatBiMTo1 add2 = p2.getAddress();
        Assert.assertNotNull(add2);

        Assert.assertEquals("bimanytoone_b", add2.getAddressId());
        Set<PersonnelBiMTo1> people2 = add2.getPeople();
        Assert.assertNotNull(people2);
        Assert.assertFalse(people2.isEmpty());
        Assert.assertEquals(2, people2.size());
    }

    /**
     * @param p1
     */
    private void assertPerson1(PersonnelBiMTo1 p1)
    {
        Assert.assertNotNull(p1);
        Assert.assertEquals("bimanytoone_1", p1.getPersonId());
        Assert.assertEquals("Amresh", p1.getPersonName());

        HabitatBiMTo1 add = p1.getAddress();
        Assert.assertNotNull(add);

        Assert.assertEquals("bimanytoone_b", add.getAddressId());
        Set<PersonnelBiMTo1> people = add.getPeople();
        Assert.assertNotNull(people);
        Assert.assertFalse(people.isEmpty());
        Assert.assertEquals(2, people.size());
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
                    ByteBuffer.wrap("drop table \"PersonnelBiMTo1\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"PersonnelBiMTo1\" ( \"PERSON_ID\" text PRIMARY KEY,  \"PERSON_NAME\" text, \"ADDRESS_ID\" text)",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelBiMTo1\" ( \"PERSON_NAME\")", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelBiMTo1\" ( \"ADDRESS_ID\")", keyspaceName);
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
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("drop table \"HabitatBiMTo1\"".getBytes("UTF-8")),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli.executeCqlQuery(
                "create table \"HabitatBiMTo1\" ( \"ADDRESS_ID\" text PRIMARY KEY,  \"STREET\" text)", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatBiMTo1\" ( \"STREET\")", keyspaceName);
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
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelBiMTo1 (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(150), ADDRESS_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PersonnelBiMTo1");
//            cli.update("DROP TABLE KUNDERATESTS.PersonnelBiMTo1");
//            cli.update("CREATE TABLE KUNDERATESTS.PersonnelBiMTo1 (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(150), ADDRESS_ID VARCHAR(150))");
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
           // cli.update("CREATE TABLE KUNDERATESTS.HabitatBiMTo1 (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(150))");
            cli.update("CREATE TABLE KUNDERATESTS.HabitatBiMTo1 (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256),PERSON_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.HabitatBiMTo1");
//            cli.update("DROP TABLE KUNDERATESTS.HabitatBiMTo1");
//            cli.update("CREATE TABLE KUNDERATESTS.HabitatBiMTo1 (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(150))");
        }
    }
}
