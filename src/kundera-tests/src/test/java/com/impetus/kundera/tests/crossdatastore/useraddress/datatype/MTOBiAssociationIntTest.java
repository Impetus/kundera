/*******************************************************************************
* * Copyright 2012 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
package com.impetus.kundera.tests.crossdatastore.useraddress.datatype;

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
import com.impetus.kundera.tests.crossdatastore.useraddress.TwinAssociation;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.HabitatBiMTo1Char;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.PersonnelBiMTo1Int;

public class MTOBiAssociationIntTest extends TwinAssociation
{
    public static final String[] ALL_PUs_UNDER_TEST = new String[] { "addCassandra", "addMongo", 
            "addCouchdb" };

    /**
* Inits the.
*/
    @BeforeClass
    public static void init() throws Exception
    {
        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelBiMTo1Int.class);
        clazzz.add(HabitatBiMTo1Char.class);
        init(clazzz, ALL_PUs_UNDER_TEST);
    }

    /**
* Sets the up.
*
* @throws Exception
* the exception
*/
    @Before
    public void setUp() throws Exception
    {
        setUpInternal("HabitatBiMTo1Char", "PersonnelBiMTo1Int");
    }

    @Test
    public void testCRUD()
    {
        tryOperation(ALL_PUs_UNDER_TEST);
    }

    @Override
    protected void insert()
    {
        PersonnelBiMTo1Int person1 = new PersonnelBiMTo1Int();
        person1.setPersonId(12345);
        person1.setPersonName("Amresh");

        PersonnelBiMTo1Int person2 = new PersonnelBiMTo1Int();
        person2.setPersonId(12346);
        person2.setPersonName("Vivek");

        HabitatBiMTo1Char address = new HabitatBiMTo1Char();
        address.setAddressId('A');
        address.setStreet("AAAAAAAAAAAAA");

        person1.setAddress(address);
        person2.setAddress(address);

        Set<PersonnelBiMTo1Int> persons = new HashSet<PersonnelBiMTo1Int>();
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
        PersonnelBiMTo1Int p1 = (PersonnelBiMTo1Int) dao.findPerson(PersonnelBiMTo1Int.class, 12345);
        assertPerson1(p1);

        // Find Person 2
        PersonnelBiMTo1Int p2 = (PersonnelBiMTo1Int) dao.findPerson(PersonnelBiMTo1Int.class, 12346);
        assertPerson2(p2);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelBiMTo1Int p = (PersonnelBiMTo1Int) dao.findPersonByIdColumn(PersonnelBiMTo1Int.class, 12345);
        assertPerson1(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelBiMTo1Int> persons = dao.findPersonByName(PersonnelBiMTo1Int.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelBiMTo1Int person = persons.get(0);
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
    private void assertPerson2(PersonnelBiMTo1Int p2)
    {
        Assert.assertNotNull(p2);
        Assert.assertEquals(12346, p2.getPersonId());
        Assert.assertEquals("Vivek", p2.getPersonName());

        HabitatBiMTo1Char add2 = p2.getAddress();
        Assert.assertNotNull(add2);

        Assert.assertEquals('A', add2.getAddressId());
        Set<PersonnelBiMTo1Int> people2 = add2.getPeople();
        Assert.assertNotNull(people2);
        Assert.assertFalse(people2.isEmpty());
        Assert.assertEquals(2, people2.size());
    }

    /**
* @param p1
*/
    private void assertPerson1(PersonnelBiMTo1Int p1)
    {
        Assert.assertNotNull(p1);
        Assert.assertEquals(12345, p1.getPersonId());
        Assert.assertEquals("Amresh", p1.getPersonName());

        HabitatBiMTo1Char add = p1.getAddress();
        Assert.assertNotNull(add);

        Assert.assertEquals('A', add.getAddressId());
        Set<PersonnelBiMTo1Int> people = add.getPeople();
        Assert.assertNotNull(people);
        Assert.assertFalse(people.isEmpty());
        Assert.assertEquals(2, people.size());
    }

    /**
* Tear down.
*
* @throws Exception
* the exception
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
                    ByteBuffer.wrap("drop table \"PersonnelBiMTo1Int\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"PersonnelBiMTo1Int\" ( \"PERSON_ID\" int PRIMARY KEY,  \"PERSON_NAME\" text, \"ADDRESS_ID\" text)",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelBiMTo1Int\" ( \"PERSON_NAME\")", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelBiMTo1Int\" ( \"ADDRESS_ID\")", keyspaceName);
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
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("drop table \"HabitatBiMTo1Char\"".getBytes("UTF-8")),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli.executeCqlQuery(
                "create table \"HabitatBiMTo1Char\" ( \"ADDRESS_ID\" text PRIMARY KEY,  \"STREET\" text)", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatBiMTo1Char\" ( \"STREET\")", keyspaceName);
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
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelBiMTo1Int (PERSON_ID INTEGER PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(6))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PersonnelBiMTo1Int");
            cli.update("DROP TABLE KUNDERATESTS.PersonnelBiMTo1Int");
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelBiMTo1Int (PERSON_ID INTEGER PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(6))");
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
            cli.update("CREATE TABLE KUNDERATESTS.ADDRESS (ADDRESS_ID VARCHAR(6) PRIMARY KEY, STREET VARCHAR(256))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.HabitatBiMTo1Char");
            cli.update("DROP TABLE KUNDERATESTS.HabitatBiMTo1Char");
            cli.update("CREATE TABLE KUNDERATESTS.HabitatBiMTo1Char (ADDRESS_ID VARCHAR(6) PRIMARY KEY, STREET VARCHAR(256))");
        }
    }
}