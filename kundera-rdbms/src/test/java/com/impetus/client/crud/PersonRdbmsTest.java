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
package com.impetus.client.crud;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PersonRdbmsTest extends BaseTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    private Map<Object, Object> col;

    private RDBMSCli cli;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("testHibernate");
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    @Test
    public void onInsertRdbms() throws Exception
    {
        cli = new RDBMSCli("testdb");
        cli.createSchema("testdb");
        // cli.update("USE testdb");
        cli.update("CREATE TABLE TESTDB.PERSON (PERSON_ID VARCHAR(9) PRIMARY KEY, PERSON_NAME VARCHAR(256), AGE INTEGER)");
        Object p1 = prepareRDBMSInstance("1", 10);
        Object p2 = prepareRDBMSInstance("2", 20);
        Object p3 = prepareRDBMSInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        PersonRDBMS personRDBMS = findById(PersonRDBMS.class, "1", em);
        Assert.assertNotNull(personRDBMS);
        Assert.assertEquals("vivek", personRDBMS.getPersonName());
        assertFindWithoutWhereClause(em, "PersonRDBMS", PersonRDBMS.class);
        assertFindByName(em, "PersonRDBMS", PersonRDBMS.class, "vivek", "personName");
        assertFindByNameAndAge(em, "PersonRDBMS", PersonRDBMS.class, "vivek", "10", "personName");
        assertFindByNameAndAgeGTAndLT(em, "PersonRDBMS", PersonRDBMS.class, "vivek", "10", "20", "personName");
        assertFindByNameAndAgeBetween(em, "PersonRDBMS", PersonRDBMS.class, "vivek", "10", "15", "personName");
        assertFindByRange(em, "PersonRDBMS", PersonRDBMS.class, "1", "2", "personId");
    }

    // @Test
    public void onMergeRdbms()
    {
        Object p1 = prepareRDBMSInstance("1", 10);
        Object p2 = prepareRDBMSInstance("2", 20);
        Object p3 = prepareRDBMSInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        col.put("1", p1);
        col.put("2", p2);
        col.put("3", p3);
        PersonRDBMS personRDBMS = findById(PersonRDBMS.class, "1", em);
        Assert.assertNotNull(personRDBMS);
        Assert.assertEquals("vivek", personRDBMS.getPersonName());
        personRDBMS.setPersonName("Newvivek");

        em.merge(personRDBMS);
        assertOnMerge(em, "PersonRDBMS", PersonRDBMS.class, "vivek", "newvivek", "personName");

    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {/*
      * Delete is working, but as row keys are not deleted from cassandra, so
      * resulting in issue while reading back. // Delete
      * em.remove(em.find(Person.class, "1")); em.remove(em.find(Person.class,
      * "2")); em.remove(em.find(Person.class, "3")); em.close(); emf.close();
      * em = null; emf = null;
      */
        for (Object val : col.values())
        {
            em.remove(val);
        }
        cli.update("DELETE FROM TESTDB.PERSON");
        cli.update("DROP TABLE TESTDB.PERSON");
        cli.update("DROP SCHEMA TESTDB");
        cli.closeConnection();
//        cli.dropSchema("testdb");
    }
}
