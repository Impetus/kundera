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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.AddressRDBMSMTO;
import com.impetus.client.crud.entities.PersonLazyRDBMSMTO;

public class RDBMSMTOLazyTest
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
        AddressRDBMSMTO address = new AddressRDBMSMTO();
        address.setAddressId("a");
        address.setStreet("sector 11");

        PersonLazyRDBMSMTO person1 = new PersonLazyRDBMSMTO();
        person1.setPersonId("1");
        person1.setPersonName("Kuldeep");

        PersonLazyRDBMSMTO person2 = new PersonLazyRDBMSMTO();
        person2.setPersonId("2");
        person2.setPersonName("vivek");

        person1.setAddress(address);
        person2.setAddress(address);

        em.persist(person1);
        em.persist(person2);

        em = getNewEM();

        PersonLazyRDBMSMTO foundPerson1 = em.find(PersonLazyRDBMSMTO.class, "1");
        Assert.assertNotNull(foundPerson1);
        Assert.assertNotNull(foundPerson1.getAddress());
        Assert.assertEquals("1", foundPerson1.getPersonId());
        Assert.assertEquals("Kuldeep", foundPerson1.getPersonName());
        Assert.assertEquals("a", foundPerson1.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson1.getAddress().getStreet());

        PersonLazyRDBMSMTO foundPerson2 = em.find(PersonLazyRDBMSMTO.class, "2");
        Assert.assertNotNull(foundPerson2);
        Assert.assertNotNull(foundPerson2.getAddress());
        Assert.assertEquals("2", foundPerson2.getPersonId());
        Assert.assertEquals("vivek", foundPerson2.getPersonName());
        Assert.assertEquals("a", foundPerson2.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson2.getAddress().getStreet());

        foundPerson1.setPersonName("KK");

        foundPerson2.setPersonName("vives");

        em.merge(foundPerson1);
        em.merge(foundPerson2);

        em = getNewEM();

        foundPerson1 = em.find(PersonLazyRDBMSMTO.class, "1");
        Assert.assertNotNull(foundPerson1);
        Assert.assertNotNull(foundPerson1.getAddress());
        Assert.assertEquals("1", foundPerson1.getPersonId());
        Assert.assertEquals("KK", foundPerson1.getPersonName());
        Assert.assertEquals("a", foundPerson1.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson1.getAddress().getStreet());

        foundPerson2 = em.find(PersonLazyRDBMSMTO.class, "2");
        Assert.assertNotNull(foundPerson2);
        Assert.assertNotNull(foundPerson2.getAddress());
        Assert.assertEquals("2", foundPerson2.getPersonId());
        Assert.assertEquals("vives", foundPerson2.getPersonName());
        Assert.assertEquals("a", foundPerson2.getAddress().getAddressId());
        Assert.assertEquals("sector 11", foundPerson2.getAddress().getStreet());

        em.remove(foundPerson1);
        em.remove(foundPerson2);

        foundPerson1 = em.find(PersonLazyRDBMSMTO.class, "1");
        foundPerson2 = em.find(PersonLazyRDBMSMTO.class, "2");
        
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
            cli.update("CREATE TABLE TESTDB.PERSONNEL (PERSON_ID VARCHAR(9) PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(9))");
            cli.update("CREATE TABLE TESTDB.ADDRESS (ADDRESS_ID VARCHAR(9) PRIMARY KEY, STREET VARCHAR(256))");
        }
        catch (Exception e)
        {
            
            cli.update("DELETE FROM TESTDB.PERSONNEL");
            cli.update("DELETE FROM TESTDB.ADDRESS");
            cli.update("DROP TABLE TESTDB.PERSONNEL");
            cli.update("DROP TABLE TESTDB.ADDRESS");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE TABLE TESTDB.PERSONNEL (PERSON_ID VARCHAR(9) PRIMARY KEY, PERSON_NAME VARCHAR(256), ADDRESS_ID VARCHAR(9))");
            cli.update("CREATE TABLE TESTDB.ADDRESS (ADDRESS_ID VARCHAR(9) PRIMARY KEY, STREET VARCHAR(256))");
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
