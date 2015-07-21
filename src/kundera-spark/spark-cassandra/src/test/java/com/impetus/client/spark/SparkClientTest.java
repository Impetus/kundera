/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.spark;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.client.spark.entities.Person;

/**
 * The Class SparkClientTest.
 * 
 * @author: karthikp.manchala
 */
public class SparkClientTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager entityManager;

    /** The pu. */
    private String PU = "spark_pu";

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {

        emf = Persistence.createEntityManagerFactory(PU);
        entityManager = emf.createEntityManager();
    }

    /**
     * Test persist.
     */
    @Test
    public void testPersist()
    {

        Person person = getPerson("1", "testpersist", 22, 50000.0);
        entityManager.persist(person);

    }

    /**
     * Test find.
     */
    @Test
    public void testFind()
    {

        Person person = entityManager.find(Person.class, "1");

        System.out.println(person.getPersonName());

    }

    /**
     * Query test.
     */
    @Test
    public void queryTest()
    {
        List<Person> results = entityManager.createNativeQuery("select * from spark_person").getResultList();
        Assert.assertNotNull(results);
        // Assert.assertEquals(3, results.size());
        print(results);

        results = entityManager.createNativeQuery("select * from spark_person where salary > 70000").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(results.get(0).getPersonName(), "Tyrion");

        results = entityManager.createNativeQuery("select * from spark_person where personName like 'Prag%'")
                .getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(results.get(0).getPersonName(), "Pragalbh");

        List aggregateResults = entityManager.createNativeQuery("select sum(salary) from spark_person").getResultList();
        Assert.assertNotNull(aggregateResults);
        Assert.assertEquals(1, aggregateResults.size());

        aggregateResults = entityManager.createNativeQuery("select count(age) from spark_person group by age")
                .getResultList();
        Assert.assertNotNull(aggregateResults);
        // Assert.assertEquals(1, aggregateResults.size());
    }

    @Test
    public void saveIntermediateResult()
    {
        String sqlString = "INSERT INTO cassandra.sparktest.ins FROM (select * from spark_person)";
        Query q = entityManager.createNativeQuery(sqlString, Person.class);
        
        System.out.println(q.executeUpdate());
    }

    /**
     * Queryand save test.
     */
    @Test
    public void queryandSaveTest()
    {
        String sqlString = "select * from spark_person where age between 20 and 25";
        Query q = entityManager.createNativeQuery(sqlString, Person.class);
        entityManager.setProperty("persist", true);
        entityManager.setProperty("keyspace", "sparktest");
        entityManager.setProperty("table", "ins");

        List persons = q.getResultList();
        print(persons);

        sqlString = "select * from spark_person where personName = 'Tyrion'";
        q = entityManager.createNativeQuery(sqlString, Person.class);
        entityManager.setProperty("persist", false);

        persons = q.getResultList();
        print(persons);

        sqlString = "select * from spark_person";
        q = entityManager.createNativeQuery(sqlString, Person.class);
        entityManager.setProperty("persist", true);
        entityManager.setProperty("persist.to", "fs");// TODO think about user
        // conventions
        entityManager.setProperty("kundera.fs.outputfile.path", "src/test/resources/csv_output/");
        //
        persons = q.getResultList();
        print(persons);

        Assert.assertNotNull(persons);
        Assert.assertFalse(persons.isEmpty());

    }

    /**
     * Gets the person.
     * 
     * @param id
     *            the id
     * @param name
     *            the name
     * @param age
     *            the age
     * @param salary
     *            the salary
     * @return the person
     */
    private Person getPerson(String id, String name, Integer age, Double salary)
    {
        Person person = new Person();
        // person.setAge(age);
        person.setPersonId(id);
        person.setPersonName(name);
        // person.setSalary(salary);
        return person;
    }

    /**
     * Prints the.
     * 
     * @param persons
     *            the persons
     */
    private void print(List persons)
    {
        for (Object obj : persons)
        {
            Person p = (Person) obj;
            System.out.println(p.getPersonId() + " " + p.getPersonName() + " " + p.getAge() + " " + p.getSalary());
        }
    }
}
