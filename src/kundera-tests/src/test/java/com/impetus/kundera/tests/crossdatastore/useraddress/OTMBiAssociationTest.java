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
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatBi1ToM;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelBi1ToM;

public class OTMBiAssociationTest extends TwinAssociation
{
    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {
        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelBi1ToM.class);
        clazzz.add(HabitatBi1ToM.class);
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
        setUpInternal("HabitatBi1ToM", "PersonnelBi1ToM");
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
        PersonnelBi1ToM personnel = new PersonnelBi1ToM();
        personnel.setPersonId("bionetomany_1");
        personnel.setPersonName("Amresh");

        Set<HabitatBi1ToM> addresses = new HashSet<HabitatBi1ToM>();
        HabitatBi1ToM address1 = new HabitatBi1ToM();
        address1.setAddressId("bionetomany_a");
        address1.setStreet("AAAAAAAAAAAAA");

        HabitatBi1ToM address2 = new HabitatBi1ToM();
        address2.setAddressId("bionetomany_b");
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
        PersonnelBi1ToM p = (PersonnelBi1ToM) dao.findPerson(PersonnelBi1ToM.class, "bionetomany_1");
        assertPerson(p);

    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelBi1ToM p = (PersonnelBi1ToM) dao.findPersonByIdColumn(PersonnelBi1ToM.class, "bionetomany_1");
        assertPerson(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelBi1ToM> persons = dao.findPersonByName(PersonnelBi1ToM.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelBi1ToM person = persons.get(0);
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
        PersonnelBi1ToM p = (PersonnelBi1ToM) dao.findPerson(PersonnelBi1ToM.class, "bionetomany_1");
        assertPerson(p);

        p.setPersonName("Saurabh");
        for (HabitatBi1ToM address : p.getAddresses())
        {
            address.setStreet("Brand New Street");
        }
        dao.merge(p);
        PersonnelBi1ToM pAfterMerge = (PersonnelBi1ToM) dao.findPerson(PersonnelBi1ToM.class, "bionetomany_1");

        assertPersonAfterUpdate(pAfterMerge);
    }

    @Override
    protected void remove()
    {
        // Find Person
        PersonnelBi1ToM p = (PersonnelBi1ToM) dao.findPerson(PersonnelBi1ToM.class, "bionetomany_1");
        assertPersonAfterUpdate(p);

        dao.remove("bionetomany_1", PersonnelBi1ToM.class);
        PersonnelBi1ToM pAfterRemoval = (PersonnelBi1ToM) dao.findPerson(PersonnelBi1ToM.class, "bionetomany_1");
        Assert.assertNull(pAfterRemoval);
    }

    private void assertPerson(PersonnelBi1ToM p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("bionetomany_1", p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        Set<HabitatBi1ToM> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (HabitatBi1ToM address : adds)
        {
            Assert.assertNotNull(address);
            PersonnelBi1ToM person = address.getPerson();
            Assert.assertNotNull(person);
            Assert.assertEquals(p.getPersonId(), person.getPersonId());
            Assert.assertEquals(p.getPersonName(), person.getPersonName());
            Assert.assertNotNull(person.getAddresses());
            Assert.assertFalse(person.getAddresses().isEmpty());
            Assert.assertEquals(2, person.getAddresses().size());
        }
    }

    private void assertPersonAfterUpdate(PersonnelBi1ToM p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals("bionetomany_1", p.getPersonId());
        Assert.assertEquals("Saurabh", p.getPersonName());

        Set<HabitatBi1ToM> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (HabitatBi1ToM address : adds)
        {
            Assert.assertNotNull(address);
            Assert.assertEquals("Brand New Street", address.getStreet());
            PersonnelBi1ToM person = address.getPerson();
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
                    ByteBuffer.wrap("drop table \"PersonnelBi1ToM\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli.executeCqlQuery(
                "create table \"PersonnelBi1ToM\" ( \"PERSON_ID\" text PRIMARY KEY,  \"PERSON_NAME\" text)",
                keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelBi1ToM\" ( \"PERSON_NAME\")", keyspaceName);
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
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("drop table \"HabitatBi1ToM\"".getBytes("UTF-8")),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"HabitatBi1ToM\" ( \"ADDRESS_ID\" text PRIMARY KEY,  \"STREET\" text, \"PERSON_ID\" text )",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatBi1ToM\" ( \"STREET\")", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatBi1ToM\" ( \"PERSON_ID\")", keyspaceName);
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
            // cli.update("CREATE TABLE KUNDERATESTS.PersonnelBi1ToM (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256))");
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelBi1ToM (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(150), ADDRESS_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PersonnelBi1ToM");
            // cli.update("DROP TABLE KUNDERATESTS.PersonnelBi1ToM");
            // cli.update("CREATE TABLE KUNDERATESTS.PersonnelBi1ToM (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256))");
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
            cli.update("CREATE TABLE KUNDERATESTS.HabitatBi1ToM (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256),PERSON_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.HabitatBi1ToM");
            // cli.update("DROP TABLE KUNDERATESTS.HabitatBi1ToM");
            // cli.update("CREATE TABLE KUNDERATESTS.HabitatBi1ToM (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256),PERSON_ID VARCHAR(150))");
        }
    }
}
