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
import com.impetus.kundera.tests.crossdatastore.useraddress.TwinAssociation;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.HabitatUni1ToMFloat;
import com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities.PersonnelUni1ToMInt;

/**
 * @author vivek.mishra
 * 
 */
public class OTMUniAssociationIntTest extends TwinAssociation
{
    private static final float _AID2 = 1235.143f;

    private static final float _AID1 = 1234.143f;

    private static final int _PID = 12345;

   //CouchDB's current supported version has an issue with handling fixed precision numbers
    public static final String[] ALL_PUs_UNDER_TEST = new String[] {"addCassandra", "addMongo",
            "oracle_kvstore"/*,"addCouchdb"*/ };

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

        if (AUTO_MANAGE_SCHEMA)
        {
            CassandraCli.initClient();
        }

        List<Class> clazzz = new ArrayList<Class>(2);
        clazzz.add(PersonnelUni1ToMInt.class);
        clazzz.add(HabitatUni1ToMFloat.class);
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
        setUpInternal("HabitatUni1ToMFloat", "PersonnelUni1ToMInt");
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
        PersonnelUni1ToMInt personnel = new PersonnelUni1ToMInt();
        personnel.setPersonId(_PID);
        personnel.setPersonName("Amresh");

        Set<HabitatUni1ToMFloat> addresses = new HashSet<HabitatUni1ToMFloat>();
        HabitatUni1ToMFloat address1 = new HabitatUni1ToMFloat();
        address1.setAddressId(_AID1);
        address1.setStreet("AAAAAAAAAAAAA");

        HabitatUni1ToMFloat address2 = new HabitatUni1ToMFloat();
        address2.setAddressId(_AID2);
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
        PersonnelUni1ToMInt p = (PersonnelUni1ToMInt) dao.findPerson(PersonnelUni1ToMInt.class, _PID);
        assertPerson(p);
    }

    @Override
    protected void findPersonByIdColumn()
    {
        PersonnelUni1ToMInt p = (PersonnelUni1ToMInt) dao.findPersonByIdColumn(PersonnelUni1ToMInt.class, _PID);
        assertPerson(p);
    }

    @Override
    protected void findPersonByName()
    {
        List<PersonnelUni1ToMInt> persons = dao.findPersonByName(PersonnelUni1ToMInt.class, "Amresh");
        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());
        Assert.assertTrue(persons.size() == 1);
        PersonnelUni1ToMInt person = persons.get(0);
        assertPerson(person);
    }

    @Override
    protected void findAddressByIdColumn()
    {
        HabitatUni1ToMFloat a = (HabitatUni1ToMFloat) dao.findAddressByIdColumn(HabitatUni1ToMFloat.class, _AID1);
        Assert.assertNotNull(a);
        Assert.assertEquals(_AID1, a.getAddressId());
        Assert.assertEquals("AAAAAAAAAAAAA", a.getStreet());
    }

    @Override
    protected void findAddressByStreet()
    {
        List<HabitatUni1ToMFloat> adds = dao.findAddressByStreet(HabitatUni1ToMFloat.class, "AAAAAAAAAAAAA");
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertTrue(adds.size() == 1);

        HabitatUni1ToMFloat a = adds.get(0);
        Assert.assertNotNull(a);
        Assert.assertEquals(_AID1, a.getAddressId());
        Assert.assertEquals("AAAAAAAAAAAAA", a.getStreet());
    }

    @Override
    protected void update()
    {
        try
        {
            PersonnelUni1ToMInt p = (PersonnelUni1ToMInt) dao.findPerson(PersonnelUni1ToMInt.class, _PID);
            Assert.assertNotNull(p);
            p.setPersonName("Saurabh");

            for (HabitatUni1ToMFloat address : p.getAddresses())
            {
                address.setStreet("Brand New Street");
            }
            dao.merge(p);
            assertPersonAfterUpdate();
        }
        catch (Exception e)
        {
            
            Assert.fail();
        }
    }

    @Override
    protected void remove()
    {
        try
        {
            // PersonnelUni1ToMInt p = (PersonnelUni1ToMInt)
            // dao.findPerson(PersonnelUni1ToMInt.class, );
            dao.remove(_PID, PersonnelUni1ToMInt.class);
            PersonnelUni1ToMInt pAfterRemoval = (PersonnelUni1ToMInt) dao.findPerson(PersonnelUni1ToMInt.class, _PID);
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
       // shutDownRdbmsServer();
        // tearDownInternal(ALL_PUs_UNDER_TEST);

    }

    /**
     * @param p
     */
    private void assertPerson(PersonnelUni1ToMInt p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(_PID, p.getPersonId());
        Assert.assertEquals("Amresh", p.getPersonName());

        Set<HabitatUni1ToMFloat> adds = p.getAddresses();
        Assert.assertNotNull(adds);
        Assert.assertFalse(adds.isEmpty());
        Assert.assertEquals(2, adds.size());

        for (HabitatUni1ToMFloat address : adds)
        {
            Assert.assertNotNull(address.getStreet());
        }
    }

    /**
     * 
     */
    private void assertPersonAfterUpdate()
    {
        PersonnelUni1ToMInt pAfterMerge = (PersonnelUni1ToMInt) dao.findPerson(PersonnelUni1ToMInt.class, _PID);
        Assert.assertNotNull(pAfterMerge);
        Assert.assertEquals("Saurabh", pAfterMerge.getPersonName());

        for (HabitatUni1ToMFloat address : pAfterMerge.getAddresses())
        {
            Assert.assertEquals("Brand New Street", address.getStreet());
        }
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
                    ByteBuffer.wrap("drop table \"PersonnelUni1ToMInt\"".getBytes("UTF-8")), Compression.NONE,
                    ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli.executeCqlQuery(
                "create table \"PersonnelUni1ToMInt\" ( \"PERSON_ID\" int PRIMARY KEY,  \"PERSON_NAME\" text)",
                keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"PersonnelUni1ToMInt\" ( \"PERSON_NAME\")", keyspaceName);
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
            CassandraCli.client.execute_cql3_query(ByteBuffer.wrap("drop table \"HabitatUni1ToMFloat\"".getBytes("UTF-8")),
                    Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception ex)
        {

        }
        CassandraCli
                .executeCqlQuery(
                        "create table \"HabitatUni1ToMFloat\" ( \"ADDRESS_ID\" float PRIMARY KEY,  \"STREET\" text, \"PERSON_ID\" int )",
                        keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatUni1ToMFloat\" ( \"STREET\")", keyspaceName);
        CassandraCli.executeCqlQuery("create index on \"HabitatUni1ToMFloat\" ( \"PERSON_ID\")", keyspaceName);
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
            cli.update("CREATE TABLE KUNDERATESTS.PERSONNEL (PERSON_ID INTEGER PRIMARY KEY, PERSON_NAME VARCHAR(150))");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.PersonnelUni1ToMInt");
            cli.update("DROP TABLE KUNDERATESTS.PersonnelUni1ToMInt");
            cli.update("CREATE TABLE KUNDERATESTS.PersonnelUni1ToMInt(PERSON_ID INTEGER PRIMARY KEY, PERSON_NAME VARCHAR(150))");
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
            cli.update("CREATE TABLE KUNDERATESTS.HabitatUni1ToMFloat (ADDRESS_ID DECIMAL(10,3) PRIMARY KEY, STREET VARCHAR(256),PERSON_ID INTEGER)");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.HabitatUni1ToMFloat");
            cli.update("DROP TABLE KUNDERATESTS.HabitatUni1ToMFloat");
            cli.update("CREATE TABLE KUNDERATESTS.HabitatUni1ToMFloat (ADDRESS_ID DECIMAL(10,3) PRIMARY KEY, STREET VARCHAR(256),PERSON_ID INTEGER)");
        }
    }

}
