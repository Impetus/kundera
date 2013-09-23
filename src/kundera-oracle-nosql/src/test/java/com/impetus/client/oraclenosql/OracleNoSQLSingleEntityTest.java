/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.oraclenosql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.oraclenosql.entities.PersonKVStore;

/**
 * Test case for CRUD and Queries on a single entity
 * 
 * @author amresh.singh
 */

public class OracleNoSQLSingleEntityTest extends OracleNoSQLTestBase
{

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        super.setUp();
    }

    @After
    public void tearDown()
    {
        super.tearDown();
    }

    @Test
    public void executeCRUDTest()
    {
        // Insert records
        persistPerson("1", "person1", 10);
        persistPerson("2", "person2", 20);
        persistPerson("3", "person3", 30);
        persistPerson("4", "person4", 40);

        // Find Records
        clearEm();
        PersonKVStore p11 = findById("1");
        assertNotNull(p11);
        assertEquals("person1", p11.getPersonName());
        assertEquals(10, p11.getAge());

        PersonKVStore p22 = findById("2");
        assertNotNull(p22);
        assertEquals("person2", p22.getPersonName());
        assertEquals(20, p22.getAge());

        PersonKVStore p33 = findById("3");
        assertNotNull(p33);
        assertEquals("person3", p33.getPersonName());
        assertEquals(30, p33.getAge());

        PersonKVStore p44 = findById("4");
        assertNotNull(p44);
        assertEquals("person4", p44.getPersonName());
        assertEquals(40, p44.getAge());

        PersonKVStore p55 = findById("5"); // Invalid records
        Assert.assertNull(p55);

        // Update records
        p11.setPersonName("person11");
        p11.setAge(100);
        updatePerson(p11);
        p22.setPersonName("person22");
        p22.setAge(200);
        updatePerson(p22);
        p33.setPersonName("person33");
        p33.setAge(300);
        updatePerson(p33);
        p44.setPersonName("person44");
        p44.setAge(400);
        updatePerson(p44);
        clearEm();
        p11 = findById("1");
        assertNotNull(p11);
        assertEquals("person11", p11.getPersonName());
        assertEquals(100, p11.getAge());

        p22 = findById("2");
        assertNotNull(p22);
        assertEquals("person22", p22.getPersonName());
        assertEquals(200, p22.getAge());

        p33 = findById("3");
        assertNotNull(p33);
        assertEquals("person33", p33.getPersonName());
        assertEquals(300, p33.getAge());

        p44 = findById("4");
        assertNotNull(p44);
        assertEquals("person44", p44.getPersonName());
        assertEquals(400, p44.getAge());

        // Delete Records
        deletePerson(p11);
        deletePerson(p22);
        deletePerson(p33);
        deletePerson(p44);

        clearEm();
        Assert.assertNull(findById("1"));
        Assert.assertNull(findById("2"));
        Assert.assertNull(findById("3"));
        Assert.assertNull(findById("4"));

    }

    @Test
    public void executeJPAQueriesTest()
    {
        // Insert records
        persistPerson("1", "person1", 10);
        persistPerson("2", "person2", 20);
        persistPerson("3", "person3", 30);
        persistPerson("4", "person4", 40);

        clearEm();
        // Select query, without where clause
        String findWithOutWhereClause = "Select p from PersonKVStore p";
        List<PersonKVStore> results = executeSelectQuery(findWithOutWhereClause);
        Assert.assertEquals(4, results.size());

        clearEm();
        // Select query with where clause on single non-ID column
        String findByName = "Select p from PersonKVStore p where p.personName=:personName";
        Map<Object, Object> params = new HashMap<Object, Object>();
        params.put("personName", "person1");
        results = executeSelectQuery(findByName, params);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("1", results.get(0).getPersonId());
        Assert.assertEquals("person1", results.get(0).getPersonName());
        Assert.assertEquals(10, results.get(0).getAge());

        clearEm();
        // Select query with where clause on ID column
        String findById = "Select p from PersonKVStore p where p.personId=:personId";
        params = new HashMap<Object, Object>();
        params.put("personId", "2");
        results = executeSelectQuery(findById, params);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("2", results.get(0).getPersonId());
        Assert.assertEquals("person2", results.get(0).getPersonName());
        Assert.assertEquals(20, results.get(0).getAge());

        clearEm();
        // Select query with where clause on ID column and non-ID column with
        // AND operator
        String findByIdAndAge = "Select p from PersonKVStore p where p.personId=:personId AND p.age=:age";
        params = new HashMap<Object, Object>();
        params.put("personId", "3");
        params.put("age", 30);
        results = executeSelectQuery(findByIdAndAge, params);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("3", results.get(0).getPersonId());
        Assert.assertEquals("person3", results.get(0).getPersonName());
        Assert.assertEquals(30, results.get(0).getAge());

        clearEm();
        // Select query with where clause on ID column and non-ID column with
        // AND operator (no record)
        params = new HashMap<Object, Object>();
        params.put("personId", "1");
        params.put("age", 30);
        results = executeSelectQuery(findByIdAndAge, params);
        Assert.assertEquals(0, results.size());

        // OR queries and numeric searches are not supported for Lucene as of
        // now, and hence are not to be
        // tested in that case
        if (!isLuceneIndexingEnabled())
        {
            clearEm();
            // Select query with where clause on ID column and non-ID column
            // with OR operator
            findByIdAndAge = "Select p from PersonKVStore p where p.personId=:personId OR p.age=:age";
            params = new HashMap<Object, Object>();
            params.put("personId", "1");
            params.put("age", 30);
            results = executeSelectQuery(findByIdAndAge, params);
            Assert.assertEquals(2, results.size());

            clearEm();
            // Select query with where clause on ID column and non-ID column
            // (greater than operator) with OR operator
            findByIdAndAge = "Select p from PersonKVStore p where p.personId=:personId OR p.age >:age";
            params = new HashMap<Object, Object>();
            params.put("personId", "1");
            params.put("age", 20);
            results = executeSelectQuery(findByIdAndAge, params);
            Assert.assertEquals(3, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with OR operator
            String findByAgeOR = "Select p from PersonKVStore p where p.age>:min OR p.age<=:max";
            params = new HashMap<Object, Object>();
            params.put("min", 30);
            params.put("max", 20);
            results = executeSelectQuery(findByAgeOR, params);
            Assert.assertEquals(3, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with AND operator
            String findByAge = "Select p from PersonKVStore p where p.age>=:min AND p.age<=:max";
            params = new HashMap<Object, Object>();
            params.put("min", 20);
            params.put("max", 30);
            results = executeSelectQuery(findByAge, params);
            Assert.assertEquals(2, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with AND operator
            String findByAgeLTGT = "Select p from PersonKVStore p where p.age<=:start AND p.age>:end";
            params = new HashMap<Object, Object>();
            params.put("start", 40);
            params.put("end", 15);
            results = executeSelectQuery(findByAgeLTGT, params);
            Assert.assertEquals(3, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with OR operator
            String findAgeByBetween = "Select p from PersonKVStore p where p.age between :min AND :max";
            params = new HashMap<Object, Object>();
            params.put("min", 20);
            params.put("max", 40);
            results = executeSelectQuery(findAgeByBetween, params);
            Assert.assertEquals(3, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with OR operator
            String findPersonIdBetween = "Select p from PersonKVStore p where p.personId between :min AND :max";
            params = new HashMap<Object, Object>();
            params.put("min", "2");
            params.put("max", "4");
            results = executeSelectQuery(findPersonIdBetween, params);
            Assert.assertEquals(3, results.size());

        }

        clearEm();
        String findSelective = "Select p.age from PersonKVStore p";
        results = executeSelectQuery(findSelective);
        Assert.assertEquals(4, results.size());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());

        // Named Query
        clearEm();
        params = new HashMap<Object, Object>();
        params.put("age", 30);
        results = executeNamedQuery("findByAge", params);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("3", results.get(0).getPersonId());
        Assert.assertEquals("person3", results.get(0).getPersonName());
        Assert.assertEquals(30, results.get(0).getAge());

        clearEm();
        params = new HashMap<Object, Object>();
        params.put(1, "person3");
        results = executeNamedQuery("findByName", params);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("3", results.get(0).getPersonId());
        Assert.assertEquals("person3", results.get(0).getPersonName());
        Assert.assertEquals(30, results.get(0).getAge());

        // Delete by query.
        String deleteQuery = "Delete from PersonKVStore p";
        int updateCount = executeDMLQuery(deleteQuery);
        Assert.assertEquals(4, updateCount);

        clearEm();
        Assert.assertEquals(null, findById("1"));
        Assert.assertEquals(null, findById("2"));
        Assert.assertEquals(null, findById("3"));
        Assert.assertEquals(null, findById("4"));
    }

    protected void persistPerson(String personId, String personName, int age)
    {
        Object p = preparePerson(personId, age, personName);
        persist(p);
    }

    protected PersonKVStore preparePerson(String rowKey, int age, String name)
    {
        PersonKVStore person = new PersonKVStore();
        person.setPersonId(rowKey);
        person.setPersonName(name);
        person.setAge(age);
        return person;
    }

    protected PersonKVStore findById(Object personId)
    {
        return (PersonKVStore) find(PersonKVStore.class, personId);
    }

    protected void updatePerson(PersonKVStore person)
    {
        update(person);
    }

    protected <E extends Object> void assertFindByNameAndAge(EntityManager em, String clazz, E e, String name,
            String minVal, String fieldName)
    {
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + name + " and p.AGE = "
                + minVal);
        List<E> results = q.getResultList();
        assertNotNull(results);
        assertTrue(!results.isEmpty());
        assertEquals(1, results.size());
    }

    protected <E extends Object> void assertFindByName(EntityManager em, String clazz, E e, String name,
            String fieldName)
    {

        String query = "Select p from " + clazz + " p where p." + fieldName + " = " + name;
        // // find by name.
        Query q = em.createQuery(query);
        List<E> results = q.getResultList();
        assertNotNull(results);
        assertTrue(!results.isEmpty());
        assertEquals(2, results.size());

    }

    protected <E extends Object> void assertFindByNameAndAgeGTAndLT(EntityManager em, String clazz, E e, String name,
            String minVal, String maxVal, String fieldName)
    {
        // // // find by name, age clause
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + name + " and p.AGE > "
                + minVal + " and p.AGE < " + maxVal);
        List<E> results = q.getResultList();
        assertNotNull(results);
        // assertTrue(!results.isEmpty());
        assertEquals(2, results.size());
    }

    protected void deletePerson(PersonKVStore person)
    {
        delete(person);
    }
}
