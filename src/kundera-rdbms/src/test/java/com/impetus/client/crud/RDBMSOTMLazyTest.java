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

import com.impetus.client.crud.entities.AddressRDBMSOTM;
import com.impetus.client.crud.entities.PersonLazyRDBMSOTM;

/**
 * @author impadmin
 * 
 */
public class RDBMSOTMLazyTest
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
    public void testCRUD()
    {
        AddressRDBMSOTM address1 = new AddressRDBMSOTM();
        address1.setAddressId("a");
        address1.setStreet("sector 11");

        AddressRDBMSOTM address2 = new AddressRDBMSOTM();
        address2.setAddressId("b");
        address2.setStreet("sector 12");

        Set<AddressRDBMSOTM> addresses = new HashSet<AddressRDBMSOTM>();
        addresses.add(address1);
        addresses.add(address2);

        PersonLazyRDBMSOTM person = new PersonLazyRDBMSOTM();
        person.setPersonId("1");
        person.setPersonName("Kuldeep");
        person.setAddresses(addresses);

        em.persist(person);

        em = getNewEM();

        PersonLazyRDBMSOTM foundPerson = em.find(PersonLazyRDBMSOTM.class, "1");
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddresses());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson.getPersonName());

        int counter = 0;
        for (AddressRDBMSOTM address : foundPerson.getAddresses())
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

        Assert.assertEquals(2, counter);

        foundPerson.setPersonName("KK");

        em.merge(foundPerson);

        em = getNewEM();

        foundPerson = em.find(PersonLazyRDBMSOTM.class, "1");
        Assert.assertNotNull(foundPerson);
        Assert.assertNotNull(foundPerson.getAddresses());
        Assert.assertEquals("1", foundPerson.getPersonId());
        Assert.assertEquals("KK", foundPerson.getPersonName());

        counter = 0;
        for (AddressRDBMSOTM address : foundPerson.getAddresses())
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

        Assert.assertEquals(2, counter);

        em.remove(foundPerson);

        foundPerson = em.find(PersonLazyRDBMSOTM.class, "1");
        Assert.assertNull(foundPerson);
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
            cli.update("CREATE TABLE TESTDB.ADDRESS (ADDRESS_ID VARCHAR(9) PRIMARY KEY, STREET VARCHAR(256), PERSON_ID VARCHAR(9))");
        }
        catch (Exception e)
        {
            
            cli.update("DELETE FROM TESTDB.PERSONNEL");
            cli.update("DELETE FROM TESTDB.ADDRESS");
            cli.update("DROP TABLE TESTDB.PERSONNEL");
            cli.update("DROP TABLE TESTDB.ADDRESS");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE TABLE TESTDB.PERSONNEL (PERSON_ID VARCHAR(9) PRIMARY KEY, PERSON_NAME VARCHAR(256))");
            cli.update("CREATE TABLE TESTDB.ADDRESS (ADDRESS_ID VARCHAR(9) PRIMARY KEY, STREET VARCHAR(256), PERSON_ID VARCHAR(9))");
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
            cli.update("DROP TABLE TESTDB.PERSONNEL");
            cli.update("DROP TABLE TESTDB.ADDRESS");
            cli.update("DROP SCHEMA TESTDB");
            cli.closeConnection();
        }
        catch (Exception e)
        {
            // Nothing to do
        }
    }
}
