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
package com.impetus.client.es;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.es.PersonES.Day;
import com.impetus.kundera.PersistenceProperties;

/**
 * The Class PersonESTest.
 * 
 * @author vivek.mishra junit to demonstrate ESQuery implementation.
 */
public class PersonESTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The node. */
    private static Node node = null;

    /** The person1. */
    private PersonES person1, person2, person3, person4;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9300);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    /**
     * Setup.
     */
    @Before
    public void setup()
    {

        emf = Persistence.createEntityManagerFactory("es-pu");
        em = emf.createEntityManager();
        init();
    }

    /**
     * Test with batch.
     * 
     * @throws NoSuchFieldException
     *             the no such field exception
     * @throws SecurityException
     *             the security exception
     * @throws IllegalArgumentException
     *             the illegal argument exception
     * @throws IllegalAccessException
     *             the illegal access exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Test
    public void testWithBatch() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException, InterruptedException
    {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9300");
        props.put(PersistenceProperties.KUNDERA_BATCH_SIZE, 10);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("es-pu", props);
        EntityManager em = emf.createEntityManager();
        // purge();

        PersonES person = new PersonES();

        for (int i = 1; i <= 25; i++)
        {
            person.setAge(i);
            person.setDay(Day.FRIDAY);
            person.setPersonId(i + "");
            person.setPersonName("vivek" + i);
            em.persist(person);

            if (i % 10 == 0)
            {
                em.clear();
                for (int i1 = 1; i1 <= 10; i1++)
                {
                    PersonES p = em.find(PersonES.class, i1 + "");
                    Assert.assertNotNull(p);
                    Assert.assertEquals("vivek" + i1, p.getPersonName());
                }
            }

        }

        em.flush();
        em.clear();
        em.close();
        emf.close();

        // A scenario to mix and match with

        // 5 inserts, 5 updates and 5 deletes

        props.put(PersistenceProperties.KUNDERA_BATCH_SIZE, 50);
        emf = Persistence.createEntityManagerFactory("es-pu", props);
        em = emf.createEntityManager();

        for (int i = 21; i <= 25; i++)
        {
            person.setAge(i);
            person.setDay(Day.FRIDAY);
            person.setPersonId(i + "");
            person.setPersonName("vivek" + i);
            em.persist(person);
        }

        for (int i = 10; i <= 20; i++)
        {
            PersonES p = em.find(PersonES.class, i + "");

            if (i > 15)
            {
                em.remove(p);
            }
            else
            {
                p.setPersonName("updatedName" + i);
                em.merge(p);
            }

        }

        em.flush(); // explicit flush
        em.clear();

        // Assert after explicit flush

        for (int i = 10; i <= 15; i++)
        {
            if (i > 15)
            {
                Assert.assertNull(em.find(PersonES.class, i + "")); // assert on
                                                                    // removed
            }
            else
            {
                PersonES found = em.find(PersonES.class, i + "");
                Assert.assertNotNull(found);
                Assert.assertEquals("updatedName" + i, found.getPersonName());
            }
        }

        for (int i = 1; i <= 25; i++)
        {
            PersonES found = em.find(PersonES.class, i + "");
            if (found != null) // as some of record are already removed.
                em.remove(found);
        }

        // TODO: Update/delete by JPA query.
        // String deleteQuery = "Delete p from PersonES p";

        em.close();
        emf.close();
    }

    /**
     * Test specific field retrieval.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Test
    public void testSpecificFieldRetrieval() throws InterruptedException
    {
        waitThread();
        String queryWithOutAndClause = "Select p.personName,p.age from PersonES p where p.personName = 'karthik' OR p.personName = 'pragalbh' ORDER BY p.personName";
        Query nameQuery = em.createNamedQuery(queryWithOutAndClause);

        List persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(2, persons.size());
        Assert.assertEquals("karthik", ((ArrayList) persons.get(0)).get(0));
        Assert.assertEquals(10, ((ArrayList) persons.get(0)).get(1));
        Assert.assertEquals("pragalbh", ((ArrayList) persons.get(1)).get(0));
        Assert.assertEquals(20, ((ArrayList) persons.get(1)).get(1));

    }

    /**
     * Test find jpql.
     * 
     * @throws InterruptedException
     *             the interrupted exception
     */
    @Test
    public void testFindJPQL() throws InterruptedException
    {
        String queryWithId = "Select p from PersonES p where p.personId = 3";
        Query nameQuery = em.createNamedQuery(queryWithId);

        List<PersonES> persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("amit", persons.get(0).getPersonName());
        Assert.assertEquals(30, persons.get(0).getAge().intValue());

        String queryWithOutAndClause = "Select p from PersonES p where p.personName = 'karthik'";
        nameQuery = em.createNamedQuery(queryWithOutAndClause);

        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("karthik", persons.get(0).getPersonName());

        String queryWithOutClause = "Select p.personName, p.personId from PersonES p";
        nameQuery = em.createNamedQuery(queryWithOutClause);

        List personsNames = nameQuery.getResultList();

        Assert.assertFalse(personsNames.isEmpty());
        Assert.assertEquals(4, personsNames.size());

        String invalidQueryWithAndClause = "Select p from PersonES p where p.personName = 'karthik' AND p.age = 34";
        nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
        persons = nameQuery.getResultList();

        Assert.assertTrue(persons.isEmpty());

        String queryWithAndClause = "Select p from PersonES p where p.personName = 'karthik' AND p.age = 10";
        nameQuery = em.createNamedQuery(queryWithAndClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals("karthik", persons.get(0).getPersonName());

        String queryWithORClause = "Select p from PersonES p where p.personName = 'karthik' OR p.personName = 'amit'";
        nameQuery = em.createNamedQuery(queryWithORClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(2, persons.size());

        String invalidQueryWithORClause = "Select p from PersonES p where p.personName = 'amit' OR p.personName = 'lkl'";
        nameQuery = em.createNamedQuery(invalidQueryWithORClause);
        persons = nameQuery.getResultList();

        Assert.assertFalse(persons.isEmpty());
        Assert.assertEquals(1, persons.size());
        // TODO: >,<,>=,<=

        String notConditionOnRowKey = "Select p from PersonES p where p.personId <> '1'";
        nameQuery = em.createNamedQuery(notConditionOnRowKey);
        persons = nameQuery.getResultList();

        assertResultList(persons, person2, person3, person4);

        String notConditionOnNonRowKey = "Select p from PersonES p where p.personName <> 'amit'";
        nameQuery = em.createNamedQuery(notConditionOnNonRowKey);
        persons = nameQuery.getResultList();

        assertResultList(persons, person1, person2, person4);

        String notConditionOnNonRowKeyWithAnd = "Select p from PersonES p where p.personName <> 'amit' and p.personId > 2";
        nameQuery = em.createNamedQuery(notConditionOnNonRowKeyWithAnd);
        persons = nameQuery.getResultList();

        assertResultList(persons, person4);

        String notConditionOnNonRowKeyWithOr = "Select p from PersonES p where p.personName <> 'amit' or p.personId <> 3";
        nameQuery = em.createNamedQuery(notConditionOnNonRowKeyWithOr);
        persons = nameQuery.getResultList();

        assertResultList(persons, person1, person2, person4);

        testCount();
        testCountWithField();
        testCountWithWhere();
        testCountWithWhereAnd();
        testCountWithWhereNullAnd();
        testCountWithWhereOr();
        testCountWithNot();
        testCountWithIn();
        testCountWithInString();
        testInWithStringArray();
        testInWithIntegerArray();
        testInWithListPositionalParameter();
        testInWithStringList();
        testInWithIntegerList();
        testInWithObjectList();
        testInWithBlankList();
        testInWithIntegerSet();
        testInWithStringSet();
        testInWithIntegerValues();
        testInWithStringValues();
        testInWithOrClause();
        testInWithAndClause();
        testFieldWithInClause();
        testMinWithIn();
        testInWithBlankValues();
    }

    /**
     * Test not with delete.
     */
    @Test
    public void testNotWithDelete()
    {
        String queryString = "delete from PersonES p where p.personId <> 2";
        Query query = em.createQuery(queryString);
        int rowUpdataCount = query.executeUpdate();
        waitThread();
        Assert.assertEquals(3, rowUpdataCount);

        List<PersonES> resultList = em.createQuery("Select p from PersonES p").getResultList();
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals("2", resultList.get(0).getPersonId());
    }

    /**
     * Test not with update.
     */
    @Test
    public void testNotWithUpdate()
    {
        String notConditionUpdateQuery = "update PersonES p set p.age = 50 where p.personName <> 'amit'";
        Query updateQuery = em.createQuery(notConditionUpdateQuery);
        int updateCount = updateQuery.executeUpdate();

        Assert.assertEquals(3, updateCount);
        PersonES person = em.find(PersonES.class, "1");
        Assert.assertEquals(50, person.getAge().intValue());

        person = em.find(PersonES.class, "2");
        Assert.assertEquals(50, person.getAge().intValue());

        person = em.find(PersonES.class, "4");
        Assert.assertEquals(50, person.getAge().intValue());
    }

    /**
     * Test count.
     */
    public void testCount()
    {
        String queryString = "Select count(p) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(4, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test count with field.
     */
    public void testCountWithField()
    {
        String queryString = "Select count(p.age) from PersonES p";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(4, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test count with where.
     */
    public void testCountWithWhere()
    {
        String queryString = "Select count(p.age) from PersonES p where p.age > 25";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(2, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test count with where and.
     */
    public void testCountWithWhereAnd()
    {
        String queryString = "Select count(p.age) from PersonES p where p.age > 25 and p.personName = 'amit'";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(1, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test count with where null and.
     */
    public void testCountWithWhereNullAnd()
    {
        String queryString = "Select count(p.age) from PersonES p where p.age < 25 and p.personName = 'amit'";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(0, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test count with where or.
     */
    public void testCountWithWhereOr()
    {
        String queryString = "Select count(p) from PersonES p where p.age < 25 or p.personName = 'amit'";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(3, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test count with not.
     */
    public void testCountWithNot()
    {
        String queryString = "Select count(p.age) from PersonES p where p.personName <> 'amit'";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(3, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test count with in.
     */
    public void testCountWithIn()
    {
        String queryString = "Select count(p.age) from PersonES p where p.age In (20, 30)";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(2, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test count with in string.
     */
    public void testCountWithInString()
    {
        String queryString = "Select count(p.age) from PersonES p where p.personName IN ('amit', 'dev', 'lilkl')";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(2, ((Long) resultList.get(0)).intValue());
    }

    /**
     * Test in with string array.
     */
    public void testInWithStringArray()
    {
        String queryString = "Select p from PersonES p where p.personId IN :values";
        Query query = em.createQuery(queryString);
        String values[] = { "3", "4", "1" };
        query.setParameter("values", values);
        List resultList = query.getResultList();

        assertResultList(resultList, person1, person3, person4);
    }

    /**
     * Test in with integer array.
     */
    public void testInWithIntegerArray()
    {
        String queryString = "Select p from PersonES p where p.age IN :values";
        Query query = em.createQuery(queryString);
        Integer values[] = { 10, 20, 50 };
        query.setParameter("values", values);
        List resultList = query.getResultList();

        assertResultList(resultList, person1, person2);
    }

    /**
     * Test in with list positional parameter.
     */
    public void testInWithListPositionalParameter()
    {
        String queryString = "Select p from PersonES p where p.age IN ?1";
        Query query = em.createQuery(queryString);
        List<Integer> inputList = new ArrayList();
        inputList.add(20);
        inputList.add(30);
        query.setParameter(1, inputList);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person2, person3);
    }

    /**
     * Test in with string list.
     */
    public void testInWithStringList()
    {
        String queryString = "Select p from PersonES p where p.personId IN :list";
        Query query = em.createQuery(queryString);
        ArrayList<String> input = new ArrayList<String>();
        input.add("2");
        input.add("3");
        query.setParameter("list", input);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person2, person3);
    }

    /**
     * Test in with integer list.
     */
    public void testInWithIntegerList()
    {
        String queryString = "Select p from PersonES p where p.age IN :list";
        Query query = em.createQuery(queryString);
        ArrayList<Integer> input = new ArrayList<Integer>();
        input.add(20);
        input.add(40);
        query.setParameter("list", input);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person2, person4);
    }

    /**
     * Test in with object list.
     */
    public void testInWithObjectList()
    {
        String queryString = "Select p from PersonES p where p.personId IN :list";
        Query query = em.createQuery(queryString);
        List inputList = new ArrayList();
        inputList.add("2");
        inputList.add("3");
        query.setParameter("list", inputList);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person2, person3);
    }

    /**
     * Test in with blank list.
     */
    public void testInWithBlankList()
    {
        String queryString = "Select p from PersonES p where p.age IN :list";
        Query query = em.createQuery(queryString);
        ArrayList input = new ArrayList();
        query.setParameter("list", input);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList);
    }

    /**
     * Test in with integer set.
     */
    public void testInWithIntegerSet()
    {
        String queryString = "Select p from PersonES p where p.age IN :set";
        Query query = em.createQuery(queryString);
        Set<Integer> inputSet = new HashSet<Integer>();
        inputSet.add(10);
        inputSet.add(30);
        query.setParameter("set", inputSet);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person1, person3);
    }

    /**
     * Test in with string set.
     */
    public void testInWithStringSet()
    {
        String queryString = "Select p from PersonES p where p.personId IN :set";
        Query query = em.createQuery(queryString);
        Set<String> input = new HashSet<String>();
        input.add("2");
        input.add("3");
        query.setParameter("set", input);
        List<PersonES> resultList = query.getResultList();

        assertResultList(resultList, person2, person3);
    }

    /**
     * Test in with integer values.
     */
    public void testInWithIntegerValues()
    {
        String queryString = "Select p from PersonES p where p.age IN ( 10, 20)";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        assertResultList(resultList, person1, person2);
    }

    /**
     * Test in with string values.
     */
    public void testInWithStringValues()
    {
        String queryString = "Select p from PersonES p where p.personId IN ( '2', '3','10')";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        assertResultList(resultList, person2, person3);
    }

    /**
     * Test in with or clause.
     */
    public void testInWithOrClause()
    {
        String queryString = "Select p from PersonES p where p.age IN ( 10, 20) or p.age = 40";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        assertResultList(resultList, person1, person2, person4);
    }

    /**
     * Test in with and clause.
     */
    public void testInWithAndClause()
    {
        String queryString = "Select p from PersonES p where p.age IN ( 10, 20) and p.personId = '2'";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        assertResultList(resultList, person2);
    }

    /**
     * Test field with in clause.
     */
    public void testFieldWithInClause()
    {
        String queryString = "Select p.personName from PersonES p where p.personId IN ( '1', '2') and p.age = 10";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals("karthik", resultList.get(0));
    }

    /**
     * Test min with in.
     */
    public void testMinWithIn()
    {
        String queryString = "Select sum(p.age) from PersonES p where p.personId IN ( '1', '2') or p.age = 40";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        Assert.assertNotNull(resultList);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals(70.0, resultList.get(0));
    }

    /**
     * Test in with blank values.
     */
    public void testInWithBlankValues()
    {
        String queryString = "Select p from PersonES p where p.personId IN ( )";
        Query query = em.createQuery(queryString);
        List resultList = query.getResultList();

        assertResultList(resultList);
    }

    /**
     * Test in with delete.
     */
    @Test
    public void testInWithDelete()
    {
        String queryString = "delete from PersonES p where p.personId IN ( '2', '4' )";
        Query query = em.createQuery(queryString);
        int deleteCount = query.executeUpdate();

        Assert.assertEquals(2, deleteCount);

        waitThread();
        queryString = "Select p from PersonES p";
        query = em.createQuery(queryString);
        List resultList = query.getResultList();

        assertResultList(resultList, person1, person3);
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        if (node != null)
            node.close();
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        purge();
        em.close();
        emf.close();

    }

    /**
     * Wait thread.
     */
    private void waitThread()
    {
        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Inits all the records.
     */
    private void init()
    {
        person1 = createPerson("1", 10, "karthik");
        person2 = createPerson("2", 20, "pragalbh");
        person3 = createPerson("3", 30, "amit");
        person4 = createPerson("4", 40, "dev");
        waitThread();
    }

    /**
     * Creates the person and persist them.
     * 
     * @param id
     *            the id
     * @param age
     *            the age
     * @param name
     *            the name
     * @return the person es
     */
    private PersonES createPerson(String id, int age, String name)
    {
        PersonES person = new PersonES();
        person.setAge(age);
        person.setDay(Day.FRIDAY);
        person.setPersonId(id);
        person.setPersonName(name);
        em.persist(person);

        return person;
    }

    /**
     * Delete all the records.
     */
    private void purge()
    {
        String deleteQuery = "delete from PersonES p";
        Query query = em.createQuery(deleteQuery);
        query.executeUpdate();
        waitThread();
    }

    /**
     * Verify each person object of result list.
     * 
     * @param resultPersonList
     *            the result person list
     * @param persons
     *            the persons
     */
    private void assertResultList(List<PersonES> resultPersonList, PersonES... persons)
    {
        boolean flag = false;

        Assert.assertNotNull(resultPersonList);
        Assert.assertEquals(persons.length, resultPersonList.size());

        for (PersonES person : persons)
        {
            flag = false;
            for (PersonES resultPerson : resultPersonList)
            {
                if (person.getPersonId().equals(resultPerson.getPersonId()))
                {
                    matchPerson(resultPerson, person);
                    flag = true;
                }
            }
            Assert.assertEquals("Person with id " + person.getPersonId() + " not found in Result list.", true, flag);
        }
    }

    /**
     * Match person to verify each field of both PersonES objects are same.
     * 
     * @param resultPerson
     *            the result person
     * @param person
     *            the person
     */
    private void matchPerson(PersonES resultPerson, PersonES person)
    {
        Assert.assertNotNull(resultPerson);
        Assert.assertEquals(person.getPersonId(), resultPerson.getPersonId());
        Assert.assertEquals(person.getPersonName(), resultPerson.getPersonName());
        Assert.assertEquals(person.getAge(), resultPerson.getAge());
        Assert.assertEquals(person.getSalary(), resultPerson.getSalary());
    }
}