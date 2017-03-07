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

import java.math.BigInteger;
import java.util.ArrayList;
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

public class PersonRdbmsTest extends BaseTest
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
        emf = Persistence.createEntityManagerFactory("testHibernate");
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    @Test
    public void onInsertRdbms() throws Exception
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
            // nothing
            // do
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
        // emf.close();
        // emf = Persistence.createEntityManagerFactory("testHibernate");

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
        
        //test Pagination
        testPagination();

        // Test IN clause.
        testINClause();

        // Test Native queries.
        testNativeQuery();
        
    }

    private void testPagination()
    {
        Query findQuery = em.createQuery("Select p from PersonRDBMS p");
        findQuery.setFirstResult(1);
        findQuery.setMaxResults(2);
        List<PersonRDBMS> allPersons = findQuery.getResultList();
        Assert.assertEquals(2, allPersons.size());
        Assert.assertEquals("2", allPersons.get(0).getPersonId());
        Assert.assertEquals("3", allPersons.get(1).getPersonId());
        
        //extremes
        findQuery.setFirstResult(5);
        findQuery.setMaxResults(10);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.isEmpty());
    }

    private void testINClause()
    {
        Query findQuery;
        List<PersonRDBMS> allPersons;
        findQuery = em.createQuery("Select p from PersonRDBMS p where p.personName IN :nameList");
        List<String> nameList = new ArrayList<String>();
        nameList.add("vivek");
        nameList.add("kk");

        findQuery.setParameter("nameList", nameList);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(3, allPersons.size());

        findQuery = em.createQuery("Select p from PersonRDBMS p where p.personName IN ?1");
        findQuery.setParameter(1, nameList);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(3, allPersons.size());

        em.close();

        em = emf.createEntityManager();

        try
        {
            findQuery = em.createQuery("Select p from PersonRDBMS p where p.personName IN :nameList");
            findQuery.setParameter("nameList", new ArrayList<String>());
            allPersons = findQuery.getResultList();
            Assert.fail();
        }
        catch (Exception e)
        {
            Assert.assertEquals(
                    "javax.persistence.PersistenceException: org.hibernate.exception.SQLGrammarException: could not prepare statement",
                    e.getMessage());
        }
        findQuery = em.createQuery("Select p from PersonRDBMS p where p.personName IN ('vivek', 'kk')");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(3, allPersons.size());

        findQuery = em.createQuery("Select p from PersonRDBMS p where p.age IN :ageList");
        List<Integer> ageList = new ArrayList<Integer>();
        ageList.add(10);
        ageList.add(25);
        findQuery.setParameter("ageList", ageList);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(1, allPersons.size());

        em.close();

        em = emf.createEntityManager();

        findQuery = em.createQuery("Select p from PersonRDBMS p where p.age IN (10 , 20)");
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(2, allPersons.size());

    }

    private void testNativeQuery()
    {

        Query findQuery;
        List<PersonRDBMS> allPersons;
        findQuery = em.createNativeQuery("Select * from testdb.PERSON where PERSON_NAME IN ('vivek' , 'kk')",
                PersonRDBMS.class);

        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(3, allPersons.size());

        findQuery = em.createNativeQuery("Select * from testdb.PERSON where AGE IN (10, 25)", PersonRDBMS.class);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(1, allPersons.size());

        findQuery = em.createNativeQuery("Select count(*) from testdb.PERSON", PersonRDBMS.class);
        allPersons = findQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(1, allPersons.size());
        Assert.assertTrue(allPersons.get(0) instanceof Map);
        Assert.assertTrue(((Map<String, Object>) allPersons.get(0)).get("C1") != null);
        Assert.assertTrue(((Map<String, Object>) allPersons.get(0)).get("C1") instanceof BigInteger);
        Assert.assertTrue(((BigInteger) ((Map<String, Object>) allPersons.get(0)).get("C1")).intValue() == 3);
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
        // cli.dropSchema("TESTDB");
    }
}
