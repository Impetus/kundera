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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatUni1To1FK;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelUni1To1FK;

/**
 * One to one association test for {@see} cassandra, mongodb,HBase and RDBMS
 * combination.
 * 
 * Script to create super column family
 * 
 * Note: As PersonnelUni1To1FK is holding embedded collection so to test as a
 * super col family. create super column family as given below: create column
 * family PersonnelUni1To1FK with comparator=UTF8Type and
 * default_validation_class=UTF8Type and key_validation_class=UTF8Type and
 * column_type=Super;
 * 
 * 
 * Note: To create as column family PersonnelUni1To1FK use as given below: create column
 * family PersonnelUni1To1FK with comparator=UTF8Type and column_metadata=[{column_name:
 * PERSON_NAME, validation_class: UTF8Type, index_type: KEYS}, {column_name:
 * AGE, validation_class: IntegerType, index_type: KEYS}]; PersonnelUni1ToM
 * 
 * @author vivek.mishra
 */

public class OTOUniAssociationTest extends TwinAssociation
{
    /**
     * Inits the.
     */
    @BeforeClass
    public static void init() throws Exception
    {
        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelUni1To1FK.class);
        clazzz.add(HabitatUni1To1FK.class);
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
        setUpInternal("HabitatUni1To1FK","PersonnelUni1To1FK");
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
            
            Assert.fail(e.getMessage());
        }
    }

    protected void insert()
    {
        try
        {
            PersonnelUni1To1FK person = new PersonnelUni1To1FK();
            HabitatUni1To1FK address = new HabitatUni1To1FK();
            person.setPersonId("unionetoonefk_1");
            person.setPersonName("Amresh");
            address.setAddressId("unionetoonefk_a");
            address.setStreet("123, New street");
            person.setAddress(address);
            dao.insert(person);
            col.add(person);
            col.add(address);
        }
        catch (Exception e)
        {
            
            Assert.fail(e.getMessage());
        }
    }

    protected void find()
    {
        // Find Person
        try
        {
            PersonnelUni1To1FK p = (PersonnelUni1To1FK) dao.findPerson(PersonnelUni1To1FK.class, "unionetoonefk_1");
            assertPersonBeforeUpdate(p);
        }
        catch (Exception e)
        {
            
            Assert.fail(e.getMessage());
        }
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelUni1To1FK p = (PersonnelUni1To1FK) dao.findPersonByIdColumn(PersonnelUni1To1FK.class,
                "unionetoonefk_1");
        assertPersonBeforeUpdate(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelUni1To1FK> persons = dao.findPersonByName(PersonnelUni1To1FK.class, "Amresh");
        assertPersons(persons);
    }

    @Override
    protected void findAddressByIdColumn()
    {
        HabitatUni1To1FK a = (HabitatUni1To1FK) dao.findAddressByIdColumn(HabitatUni1To1FK.class, "unionetoonefk_a");
        assertAddressBeforeUpdate(a);
    }

    @Override
    protected void findAddressByStreet()
    {
        List<HabitatUni1To1FK> adds = dao.findAddressByStreet(HabitatUni1To1FK.class, "123, New street");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        assertAddressBeforeUpdate(adds.get(0));
    }

    protected void update()
    {
        try
        {
            PersonnelUni1To1FK p = (PersonnelUni1To1FK) dao.findPerson(PersonnelUni1To1FK.class, "unionetoonefk_1");
            Assert.assertNotNull(p);

            // dao.merge(p); //This merge operation should do nothing since
            // nothing has changed

            // p = (PersonnelUni1To1FK) dao.findPerson(PersonnelUni1To1FK.class,
            // "unionetoonefk_1");
            p.setPersonName("Saurabh");
            HabitatUni1To1FK address = p.getAddress();
            address.setStreet("Brand New Street");
            p.setAddress(address);
            dao.merge(p);

            PersonnelUni1To1FK pAfterMerge = (PersonnelUni1To1FK) dao.findPerson(PersonnelUni1To1FK.class,
                    "unionetoonefk_1");
            assertPersonAfterUpdate(pAfterMerge);
        }
        catch (Exception e)
        {
            
            Assert.fail(e.getMessage());
        }
    }

    protected void remove()
    {
        try
        {
            PersonnelUni1To1FK p = (PersonnelUni1To1FK) dao.findPerson(PersonnelUni1To1FK.class, "unionetoonefk_1");
            Assert.assertNotNull(p);
            dao.remove("unionetoonefk_1", PersonnelUni1To1FK.class);

            PersonnelUni1To1FK pAfterRemoval = (PersonnelUni1To1FK) dao.findPerson(PersonnelUni1To1FK.class,
                    "unionetoonefk_1");
            Assert.assertNull(pAfterRemoval);
        }
        catch (Exception e)
        {
            
            Assert.fail(e.getMessage());
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
        // tearDownInternal(ALL_PUs_UNDER_TEST);
      //  shutDownRdbmsServer();
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

    }

    @Override
    public void loadDataForPERSONNEL() throws InvalidRequestException, TException, SchemaDisagreementException
    {
        String keyspaceName = "KunderaTests";
        CassandraCli.createKeySpace(keyspaceName);

        CassandraCli.client.set_keyspace(keyspaceName);
        try
        {
            CassandraCli.client.execute_cql3_query(
                    ByteBuffer.wrap("drop table \"PersonnelUni1To1FK\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"PersonnelUni1To1FK\" ( \"PERSON_ID\" text PRIMARY KEY,  \"PERSON_NAME\" text, \"ADDRESS_ID\" text)",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelUni1To1FK\" ( \"PERSON_NAME\")", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelUni1To1FK\" ( \"ADDRESS_ID\")", keyspaceName);
    }

    @Override
    public void loadDataForHABITAT() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {
        String keyspaceName = "KunderaTests";
        CassandraCli.createKeySpace(keyspaceName);

        CassandraCli.client.set_keyspace(keyspaceName);
        try
        {
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("drop table \"HabitatUni1To1FK\"".getBytes("UTF-8")),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli.executeCqlQuery(
                "create table \"HabitatUni1To1FK\" ( \"ADDRESS_ID\" text PRIMARY KEY,  \"STREET\" text)", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatUni1To1FK\" ( \"STREET\")", keyspaceName);
    }

    /**
     * @param p
     */
    private void assertPersonBeforeUpdate(PersonnelUni1To1FK p)
    {
        Assert.assertNotNull(p);

        Assert.assertEquals("unionetoonefk_1", p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        assertAddressBeforeUpdate(p.getAddress());
    }

    /**
     * @param p
     */
    private void assertAddressBeforeUpdate(HabitatUni1To1FK add)
    {
        Assert.assertNotNull(add);

        String addressId = add.getAddressId();
        String street = add.getStreet();

        Assert.assertNotNull(addressId);
        Assert.assertEquals("unionetoonefk_a", addressId);
        Assert.assertEquals("123, New street", street);
    }

    /**
     * @param pAfterMerge
     */
    private void assertPersonAfterUpdate(PersonnelUni1To1FK pAfterMerge)
    {
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());
        HabitatUni1To1FK addressAfterMerge = pAfterMerge.getAddress();
        Assert.assertNotNull(addressAfterMerge);
        Assert.assertEquals("Brand New Street", addressAfterMerge.getStreet());
    }

    /**
     * @param persons
     */
    private void assertPersons(List<PersonnelUni1To1FK> persons)
    {
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelUni1To1FK person = persons.get(0);
        assertPersonBeforeUpdate(person);
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
        // cli.update("USE testdb");
        try
        {
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelUni1To1FK (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PersonnelUni1To1FK");
//            cli.update("DROP TABLE KUNDERATESTS.PersonnelUni1To1FK");
//            cli.update("CREATE TABLE KUNDERATESTS.PersonnelUni1To1FK (PERSON_ID VARCHAR(150) PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(150))");
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
            //cli.update("CREATE TABLE KUNDERATESTS.HabitatUni1To1FK (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256))");
            cli.update("CREATE TABLE KUNDERATESTS.HabitatUni1To1FK (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(250),PERSON_ID VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.HabitatUni1To1FK");
//            cli.update("DROP TABLE KUNDERATESTS.HabitatUni1To1FK");
//            cli.update("CREATE TABLE KUNDERATESTS.HabitatUni1To1FK (ADDRESS_ID VARCHAR(150) PRIMARY KEY, STREET VARCHAR(256))");
        }
    }

}
