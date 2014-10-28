/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.PersonRDBMS;

public class RdbmsLuceneTest extends BaseTest
{

    private static final String SCHEMA = "testdb";

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
        Map<String,String> propertyMap = new HashMap<String,String>();
        propertyMap.put("index.home.dir", "lucene");
        emf = Persistence.createEntityManagerFactory("testHibernate",propertyMap);
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    @Test
    public void test() throws SQLException
    {
        try
        {
            cli = new RDBMSCli(SCHEMA);
            cli.createSchema(SCHEMA);
            cli.update("CREATE TABLE TESTDB.PERSON (PERSON_ID VARCHAR(9) PRIMARY KEY, PERSON_NAME VARCHAR(256), AGE INTEGER)");
        }
        catch (Exception e)
        {

            cli.update("DELETE FROM TESTDB.PERSON");
            cli.update("DROP TABLE TESTDB.PERSON");
            cli.update("DROP SCHEMA TESTDB");
            cli.update("CREATE TABLE TESTDB.PERSON (PERSON_ID VARCHAR(9) PRIMARY KEY, PERSON_NAME VARCHAR(256), AGE INTEGER)");
        }

        Object p1 = prepareRDBMSInstance("1", 10);
        Object p2 = prepareRDBMSInstance("2", 20);
        Object p3 = prepareRDBMSInstance("3", 15);

        Query findQuery = em.createQuery("Select p from PersonRDBMS p");
        List<PersonRDBMS> allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = em.createQuery("Select p from PersonRDBMS p where p.personName = 'vivek'");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = em.createQuery("Select p.age from PersonRDBMS p where p.personName = 'vivek'");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        em.close();
    
        em = emf.createEntityManager();

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

    @After
    public void tearDown() throws Exception
    {
        for (Object val : col.values())
        {
            em.remove(val);
        }
        em.close();
        emf.close();
        try
        {
            cli.update("DELETE FROM TESTDB.PERSON");
            cli.update("DROP TABLE TESTDB.PERSON");
            cli.update("DROP SCHEMA TESTDB");
            cli.closeConnection();
        }
        catch (Exception e)
        {
            // Nothing to do
        }
    }
    
}
