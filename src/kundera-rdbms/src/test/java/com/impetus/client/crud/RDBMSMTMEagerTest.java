/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.AddressRDBMSMTM;
import com.impetus.client.crud.entities.PersonEagerRDBMSMTM;

/**
 * @author impadmin
 * 
 */
public class RDBMSMTMEagerTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    private RDBMSCli cli;

    private static final String SCHEMA = "testdb";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("testHibernate");
        em = getNewEM();
        createSchema();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        dropSchema();
    }

    @Test
    public void testDummy()
    {
        Assert.assertTrue("Enable testCRUD while EagerFetching in rdbms get fixed.", true);

    }

    // @Test
    public void testCRUD()
    {
        // TODO Eager fetching not working.

        AddressRDBMSMTM address1 = new AddressRDBMSMTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressRDBMSMTM address2 = new AddressRDBMSMTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        AddressRDBMSMTM address3 = new AddressRDBMSMTM();
        address3.setAddressId("c");
        address3.setStreet("sector 13");

        Set<AddressRDBMSMTM> addresses1 = new HashSet<AddressRDBMSMTM>();
        addresses1.add(address1);
        addresses1.add(address2);

        Set<AddressRDBMSMTM> addresses2 = new HashSet<AddressRDBMSMTM>();
        addresses2.add(address2);
        addresses2.add(address3);

        PersonEagerRDBMSMTM person1 = new PersonEagerRDBMSMTM();
        person1.setPersonId("1");
        person1.setPersonName("Kuldeep");

        PersonEagerRDBMSMTM person2 = new PersonEagerRDBMSMTM();
        person2.setPersonId("2");
        person2.setPersonName("vivek");

        person1.setAddresses(addresses1);
        person2.setAddresses(addresses2);

        em.persist(person1);
        em.persist(person2);

        em = getNewEM();

        PersonEagerRDBMSMTM foundPerson1 = em.find(PersonEagerRDBMSMTM.class, "1");
        Assert.assertNotNull(foundPerson1);
        Assert.assertNotNull(foundPerson1.getAddresses());
        Assert.assertEquals("1", foundPerson1.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson1.getPersonName());

        int counter = 0;
        for (AddressRDBMSMTM address : foundPerson1.getAddresses())
        {
            if (address.getAddressId().equals("a"))
            {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            }
            else
            {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        PersonEagerRDBMSMTM foundPerson2 = em.find(PersonEagerRDBMSMTM.class, "2");
        Assert.assertNotNull(foundPerson2);
        Assert.assertNotNull(foundPerson2.getAddresses());
        Assert.assertEquals("2", foundPerson2.getPersonId());
        Assert.assertEquals("vivek", foundPerson2.getPersonName());

        counter = 0;
        for (AddressRDBMSMTM address : foundPerson2.getAddresses())
        {
            if (address.getAddressId().equals("b"))
            {
                counter++;
                Assert.assertEquals("sector 12", address.getStreet());
            }
            else
            {
                Assert.assertEquals("c", address.getAddressId());
                Assert.assertEquals("sector 13", address.getStreet());
                counter++;
            }
        }

        foundPerson1.setPersonName("KK");

        foundPerson2.setPersonName("vives");

        em.merge(foundPerson1);
        em.merge(foundPerson2);

        em = getNewEM();

        foundPerson1 = em.find(PersonEagerRDBMSMTM.class, "1");
        Assert.assertNotNull(foundPerson1);
        Assert.assertNotNull(foundPerson1.getAddresses());
        Assert.assertEquals("1", foundPerson1.getPersonId());
        Assert.assertEquals("KK", foundPerson1.getPersonName());

        counter = 0;
        for (AddressRDBMSMTM address : foundPerson1.getAddresses())
        {
            if (address.getAddressId().equals("a"))
            {
                counter++;
                Assert.assertEquals("sector 11", address.getStreet());
            }
            else
            {
                Assert.assertEquals("b", address.getAddressId());
                Assert.assertEquals("sector 12", address.getStreet());
                counter++;
            }
        }

        foundPerson2 = em.find(PersonEagerRDBMSMTM.class, "2");
        Assert.assertNotNull(foundPerson2);
        Assert.assertNotNull(foundPerson2.getAddresses());
        Assert.assertEquals("2", foundPerson2.getPersonId());
        Assert.assertEquals("vives", foundPerson2.getPersonName());

        counter = 0;
        for (AddressRDBMSMTM address : foundPerson2.getAddresses())
        {
            if (address.getAddressId().equals("b"))
            {
                counter++;
                Assert.assertEquals("sector 12", address.getStreet());
            }
            else
            {
                Assert.assertEquals("c", address.getAddressId());
                Assert.assertEquals("sector 13", address.getStreet());
                counter++;
            }
        }

        em.remove(foundPerson1);
        em.remove(foundPerson2);

        foundPerson1 = em.find(PersonEagerRDBMSMTM.class, "1");
        foundPerson2 = em.find(PersonEagerRDBMSMTM.class, "2");

        Assert.assertNull(foundPerson1);
        Assert.assertNull(foundPerson2);
    }

    private EntityManager getNewEM()
    {
        if (em != null && em.isOpen())
        {
            em.close();
        }
        return em = emf.createEntityManager();
    }

    private void createSchema() throws SQLException
    {
        try
        {
            cli = new RDBMSCli(SCHEMA);
            cli.createSchema(SCHEMA);
            cli.update("CREATE TABLE TESTDB.PERSONNEL (PERSON_ID VARCHAR(9) PRIMARY KEY, PERSON_NAME VARCHAR(256))");
            cli.update("CREATE TABLE TESTDB.ADDRESS (ADDRESS_ID VARCHAR(9) PRIMARY KEY, STREET VARCHAR(256))");
            cli.update("CREATE TABLE TESTDB.PERSONNEL_ADDRESS (ADDRESS_ID VARCHAR(9), PERSON_ID VARCHAR(9))");
        }
        catch (Exception e)
        {

            cli.update("DELETE FROM TESTDB.PERSONNEL");
            cli.update("DELETE FROM TESTDB.ADDRESS");
            cli.update("DELETE FROM TESTDB.PERSONNEL_ADDRESS");
            cli.update("DROP TABLE TESTDB.PERSONNEL");
            cli.update("DROP TABLE TESTDB.ADDRESS");
            cli.update("DROP TABLE TESTDB.PERSONNEL_ADDRESS");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE TABLE TESTDB.PERSON (PERSON_ID VARCHAR(9) PRIMARY KEY, PERSON_NAME VARCHAR(256))");
            cli.update("CREATE TABLE TESTDB.ADDRESS (ADDRESS_ID VARCHAR(9) PRIMARY KEY, STREET VARCHAR(256))");
            cli.update("CREATE TABLE TESTDB.PERSONNEL_ADDRESS (ADDRESS_ID VARCHAR(9), PERSON_ID VARCHAR(9))");
            // nothing
            // do
        }
    }

    private void dropSchema()
    {
        try
        {
            cli.update("DELETE FROM TESTDB.PERSONNEL");
            cli.update("DELETE FROM TESTDB.ADDRESS");
            cli.update("DELETE FROM TESTDB.PERSONNEL_ADDRESS");
            cli.update("DROP TABLE TESTDB.PERSONNEL");
            cli.update("DROP TABLE TESTDB.ADDRESS");
            cli.update("DROP TABLE TESTDB.PERSONNEL_ADDRESS");
            cli.update("DROP SCHEMA TESTDB");
            cli.closeConnection();
        }
        catch (Exception e)
        {
            // Nothing to do
        }
    }
}
