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
package com.impetus.kundera.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TypedQuery;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.query.Person.Day;

/**
 * Test case to perform simple CRUD operation.(insert, delete, merge, and
 * select)
 */
public class PersonTest
{
    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;

    protected Map propertyMap = null;

    @Before
    public void setUp() throws Exception
    {
        propertyMap = new HashMap<String, String>();
        propertyMap.put("index.home.dir", "./lucene");
        emf = Persistence.createEntityManagerFactory(PU, propertyMap);
        em = emf.createEntityManager();

    }

    @Test
    public void testInsertions() throws Exception
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);

        Query findQuery = em.createQuery("Select p from Person p", Person.class);
        List<Person> allPersons = findQuery.getResultList();
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = em.createQuery("Select p from Person p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertTrue(allPersons.isEmpty());

        findQuery = em.createQuery("Select p.age from Person p where p.personName = vivek");
        allPersons = findQuery.getResultList();
        Assert.assertTrue(allPersons.isEmpty());

        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        findQuery = em.createQuery("Select p from Person p where p.personName = vivek ORDER BY p.age ASC");
        findQuery.setFirstResult(2);
        findQuery.setMaxResults(2);
        allPersons = findQuery.getResultList();
        Assert.assertEquals(2, allPersons.size());
        Assert.assertEquals(new Integer(10), allPersons.get(0).getAge());
        Assert.assertEquals(new Integer(20), allPersons.get(1).getAge());

        Person personWithKey = new Person();
        personWithKey.setPersonId("111");
        em.persist(personWithKey);

        em.clear();
        Person p = findById(Person.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        Assert.assertEquals(Day.THURSDAY, p.getDay());

        em.clear();
        String qry = "Select p.personId,p.personName from Person p where p.personId >= 1";
        Query q = em.createQuery(qry);
        List<Person> persons = q.getResultList();

        assertFindByName(em, "Person", Person.class, "vivek", "personName");

        // Delete without WHERE clause.
        String deleteQuery = "DELETE from Person";
        q = em.createQuery(deleteQuery);
        q.executeUpdate();

    }

    @Test
    public void testUpdation() throws Exception
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        em.clear();
        Person p = findById(Person.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        // modify record.
        p.setPersonName("newvivek");
        em.merge(p);

        assertUpdation(em, "Person", Person.class, "vivek", "newvivek", "personName");
    }

    @Test
    public void testDeletion() throws Exception
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        Person p = findById(Person.class, "1", em);
        Assert.assertNotNull(p);
        Assert.assertEquals("vivek", p.getPersonName());
        em.remove(p);
        em.clear();

        TypedQuery<Person> query = em.createQuery("Select p from Person p", Person.class);

        List<Person> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
    }

    @Test
    public void testRefresh() throws Exception
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        // Check for contains
        Object pp1 = prepareData("1", 10);
        Object pp2 = prepareData("2", 20);
        Object pp3 = prepareData("3", 15);
        Assert.assertTrue(em.contains(pp1));
        Assert.assertTrue(em.contains(pp2));
        Assert.assertTrue(em.contains(pp3));

        // Check for detach
        em.detach(pp1);
        em.detach(pp2);
        Assert.assertFalse(em.contains(pp1));
        Assert.assertFalse(em.contains(pp2));
        Assert.assertTrue(em.contains(pp3));

        // Modify value in database directly, refresh and then check PC
        em.clear();
        em = emf.createEntityManager();
        Object o1 = em.find(Person.class, "1");

        em.refresh(o1);
        Object oo1 = em.find(Person.class, "1");
        Assert.assertTrue(em.contains(o1));
    }

    @Test
    public void testTypedQuery()
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        TypedQuery<Person> query = em.createQuery("Select p from Person p", Person.class);

        List<Person> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
    }

    @Test
    public void testGenericTypedQuery()
    {

        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        TypedQuery<Object> query = em.createQuery("Select p from Person p", Object.class);

        List<Object> results = query.getResultList();
        Assert.assertNotNull(query);
        Assert.assertNotNull(results);
    }

    @Test
    public void testInvalidTypedQuery()
    {
        Object p1 = prepareData("1", 10);
        Object p2 = prepareData("2", 20);
        Object p3 = prepareData("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);

        TypedQuery<PersonnelDTO> query = null;
        try
        {
            query = em.createQuery("Select p from Person p", PersonnelDTO.class);
            Assert.fail("Should have gone to catch block, as it is an invalid scenario!");
        }
        catch (IllegalArgumentException iaex)
        {
            Assert.assertNull(query);
        }
    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
        DummyDatabase.INSTANCE.dropDatabase();
    }

    private Person prepareData(String rowKey, int age)
    {
        Person o = new Person();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age);
        o.setDay(Day.THURSDAY);
        return o;
    }

    private <E extends Object> E findById(Class<E> clazz, Object rowKey, EntityManager em)
    {
        return em.find(clazz, rowKey);
    }

    private <E extends Object> void assertFindByName(EntityManager em, String clazz, E e, String name, String fieldName)
    {

        String query = "Select p from " + clazz + " p where p." + fieldName + " = " + name;
        // // find by name.
        Query q = em.createQuery(query);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
    }

    protected <E extends Object> void assertUpdation(EntityManager em, String clazz, E e, String oldName,
            String newName, String fieldName)
    {
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + oldName);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + newName);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertNotSame(oldName, getPersonName(e, results.get(0)));
        Assert.assertEquals(newName, getPersonName(e, results.get(0)));
    }

    private <E extends Object> String getPersonName(E e, Object result)
    {

        return ((Person) result).getPersonName();
    }
}
