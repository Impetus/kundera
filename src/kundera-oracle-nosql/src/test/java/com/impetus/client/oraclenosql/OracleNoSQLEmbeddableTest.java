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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.oraclenosql.entities.Office;
import com.impetus.client.oraclenosql.entities.PersonEmbeddedKVStore;
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * Test case for CRUD and query operations on an entity that contains one
 * embeddable attribute
 * 
 * @author amresh.singh
 */
public class OracleNoSQLEmbeddableTest extends OracleNoSQLTestBase
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
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
    }

    @Test
    public void executeCRUDTest()
    {

        // Insert records
        persistPerson("1", "person1", 10, new Office(1, "Company1", "Location 1"));
        persistPerson("2", "person2", 20, new Office(2, "Company2", "Location 2"));
        persistPerson("3", "person3", 30, new Office(3, "Company3", "Location 3"));
        persistPerson("4", "person4", 40, new Office(4, "Company4", "Location 4"));

        // Find Records
        clearEm();
        PersonEmbeddedKVStore p11 = findById("1");
        assertNotNull(p11);
        assertEquals("person1", p11.getPersonName());
        assertEquals(10, p11.getAge());
        Assert.assertNotNull(p11.getOffice());
        Assert.assertEquals(1, p11.getOffice().getOfficeId());
        Assert.assertEquals("Company1", p11.getOffice().getCompanyName());
        Assert.assertEquals("Location 1", p11.getOffice().getLocation());

        PersonEmbeddedKVStore p22 = findById("2");
        assertNotNull(p22);
        assertEquals("person2", p22.getPersonName());
        assertEquals(20, p22.getAge());
        Assert.assertNotNull(p22.getOffice());
        Assert.assertEquals(2, p22.getOffice().getOfficeId());
        Assert.assertEquals("Company2", p22.getOffice().getCompanyName());
        Assert.assertEquals("Location 2", p22.getOffice().getLocation());

        PersonEmbeddedKVStore p33 = findById("3");
        assertNotNull(p33);
        assertEquals("person3", p33.getPersonName());
        assertEquals(30, p33.getAge());
        Assert.assertNotNull(p33.getOffice());
        Assert.assertEquals(3, p33.getOffice().getOfficeId());
        Assert.assertEquals("Company3", p33.getOffice().getCompanyName());
        Assert.assertEquals("Location 3", p33.getOffice().getLocation());

        PersonEmbeddedKVStore p44 = findById("4");
        assertNotNull(p44);
        assertEquals("person4", p44.getPersonName());
        assertEquals(40, p44.getAge());
        Assert.assertNotNull(p44.getOffice());
        Assert.assertEquals(4, p44.getOffice().getOfficeId());
        Assert.assertEquals("Company4", p44.getOffice().getCompanyName());
        Assert.assertEquals("Location 4", p44.getOffice().getLocation());

        PersonEmbeddedKVStore p55 = findById("5"); // Invalid records
        Assert.assertNull(p55);

        // Update records
        p11.setPersonName("person11");
        p11.setAge(100);
        p11.getOffice().setCompanyName("Company11");
        updatePerson(p11);
        p22.setPersonName("person22");
        p22.setAge(200);
        p22.getOffice().setCompanyName("Company22");
        updatePerson(p22);
        p33.setPersonName("person33");
        p33.setAge(300);
        p33.getOffice().setCompanyName("Company33");
        updatePerson(p33);
        p44.setPersonName("person44");
        p44.setAge(400);
        p44.getOffice().setCompanyName("Company44");
        updatePerson(p44);
        clearEm();
        p11 = findById("1");
        assertNotNull(p11);
        assertEquals("person11", p11.getPersonName());
        assertEquals(100, p11.getAge());
        Assert.assertEquals("Company11", p11.getOffice().getCompanyName());

        p22 = findById("2");
        assertNotNull(p22);
        assertEquals("person22", p22.getPersonName());
        assertEquals(200, p22.getAge());
        Assert.assertEquals("Company22", p22.getOffice().getCompanyName());

        p33 = findById("3");
        assertNotNull(p33);
        assertEquals("person33", p33.getPersonName());
        assertEquals(300, p33.getAge());
        Assert.assertEquals("Company33", p33.getOffice().getCompanyName());

        p44 = findById("4");
        assertNotNull(p44);
        assertEquals("person44", p44.getPersonName());
        assertEquals(400, p44.getAge());
        Assert.assertEquals("Company44", p44.getOffice().getCompanyName());

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
        persistPerson("1", "person1", 10, new Office(1, "Company1", "Location 1"));
        persistPerson("2", "person2", 20, new Office(2, "Company2", "Location 2"));
        persistPerson("3", "person3", 30, new Office(3, "Company3", "Location 3"));
        persistPerson("4", "person4", 40, new Office(4, "Company4", "Location 4"));

        // Select query, without where clause
        clearEm();
        String findWithOutWhereClause = "Select p from PersonEmbeddedKVStore p";
        List<PersonEmbeddedKVStore> results = executeSelectQuery(findWithOutWhereClause);
        Assert.assertEquals(4, results.size());

        // Select query with where clause on single non-ID column
        clearEm();
        String findByName = "Select p from PersonEmbeddedKVStore p where p.personName=:personName";
        Map<Object, Object> params = new HashMap<Object, Object>();
        params.put("personName", "person1");
        results = executeSelectQuery(findByName, params);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("1", results.get(0).getPersonId());
        Assert.assertEquals("person1", results.get(0).getPersonName());
        Assert.assertEquals(10, results.get(0).getAge());

        clearEm();
        // Select query with where clause on ID column
        String findById = "Select p from PersonEmbeddedKVStore p where p.personId=:personId";
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
        String findByIdAndAge = "Select p from PersonEmbeddedKVStore p where p.personId=:personId AND p.age=:age";
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
            // with OR
            // operator
            findByIdAndAge = "Select p from PersonEmbeddedKVStore p where p.personId=:personId OR p.age=:age";
            params = new HashMap<Object, Object>();
            params.put("personId", "1");
            params.put("age", 30);
            results = executeSelectQuery(findByIdAndAge, params);
            Assert.assertEquals(2, results.size());

            clearEm();
            // Select query with where clause on ID column and non-ID column
            // (greater than operator) with OR operator
            findByIdAndAge = "Select p from PersonEmbeddedKVStore p where p.personId=:personId OR p.age >:age";
            params = new HashMap<Object, Object>();
            params.put("personId", "1");
            params.put("age", 20);
            results = executeSelectQuery(findByIdAndAge, params);
            Assert.assertEquals(3, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with AND operator
            findByIdAndAge = "Select p from PersonEmbeddedKVStore p where p.age>=:min AND p.age<=:max";
            params = new HashMap<Object, Object>();
            params.put("min", 20);
            params.put("max", 30);
            results = executeSelectQuery(findByIdAndAge, params);
            Assert.assertEquals(2, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with AND operator
            findByIdAndAge = "Select p from PersonEmbeddedKVStore p where p.age<=:start AND p.age>:end";
            params = new HashMap<Object, Object>();
            params.put("start", 40);
            params.put("end", 15);
            results = executeSelectQuery(findByIdAndAge, params);
            Assert.assertEquals(3, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with OR operator
            findByIdAndAge = "Select p from PersonEmbeddedKVStore p where p.age>:min OR p.age<=:max";
            params = new HashMap<Object, Object>();
            params.put("min", 30);
            params.put("max", 20);
            results = executeSelectQuery(findByIdAndAge, params);
            Assert.assertEquals(3, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with OR operator
            String findAgeByBetween = "Select p from PersonEmbeddedKVStore p where p.age between :min AND :max";
            params = new HashMap<Object, Object>();
            params.put("min", 20);
            params.put("max", 40);
            results = executeSelectQuery(findAgeByBetween, params);
            Assert.assertEquals(3, results.size());

            clearEm();
            // Select query with where clause on non-ID column (with comparison)
            // with OR operator
            String findPersonIdBetween = "Select p from PersonEmbeddedKVStore p where p.personId between :min AND :max";
            params = new HashMap<Object, Object>();
            params.put("min", "2");
            params.put("max", "4");
            results = executeSelectQuery(findPersonIdBetween, params);
            Assert.assertEquals(3, results.size());
        }

        clearEm();
        // Search over selective column
        String findSelective = "Select p.age from PersonEmbeddedKVStore p";
        results = executeSelectQuery(findSelective);
        Assert.assertEquals(4, results.size());
        Assert.assertNull(results.get(0).getPersonName());
        Assert.assertNotNull(results.get(0).getAge());

        clearEm();
        // Search over column within embeddable
        String findByCompanyName = "Select p from PersonEmbeddedKVStore p where p.office.companyName=:companyName";
        params = new HashMap<Object, Object>();
        params.put("companyName", "Company3");
        results = executeSelectQuery(findByCompanyName, params);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("3", results.get(0).getPersonId());
        Assert.assertEquals("person3", results.get(0).getPersonName());
        Assert.assertEquals(30, results.get(0).getAge());
        Assert.assertEquals(3, results.get(0).getOffice().getOfficeId());
        Assert.assertEquals("Company3", results.get(0).getOffice().getCompanyName());
        Assert.assertEquals("Location 3", results.get(0).getOffice().getLocation());

        // Delete by query.
        String deleteQuery = "Delete from PersonEmbeddedKVStore p";
        int updateCount = executeDMLQuery(deleteQuery);
        Assert.assertEquals(4, updateCount);

        clearEm();
        Assert.assertEquals(null, findById("1"));
        Assert.assertEquals(null, findById("2"));
        Assert.assertEquals(null, findById("3"));
        Assert.assertEquals(null, findById("4"));

    }

    protected void persistPerson(String personId, String personName, int age, Office office)
    {
        Object p = preparePerson(personId, age, personName, office);
        persist(p);
    }

    protected PersonEmbeddedKVStore preparePerson(String rowKey, int age, String name, Office office)
    {
        PersonEmbeddedKVStore person = new PersonEmbeddedKVStore();
        person.setPersonId(rowKey);
        person.setPersonName(name);
        person.setAge(age);
        person.setOffice(office);
        return person;
    }

    protected PersonEmbeddedKVStore findById(Object personId)
    {
        return (PersonEmbeddedKVStore) find(PersonEmbeddedKVStore.class, personId);
    }

    protected void updatePerson(PersonEmbeddedKVStore person)
    {
        update(person);
    }

    protected void deletePerson(PersonEmbeddedKVStore person)
    {
        delete(person);
    }

}
