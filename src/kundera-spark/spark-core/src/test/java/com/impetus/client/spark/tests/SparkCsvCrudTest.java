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
package com.impetus.client.spark.tests;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.spark.entities.Person;

/**
 * The Class SparkCsvCrudTest.
 */
public class SparkCsvCrudTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The pu. */
    private String PU = "spark_fs_pu";

    /** The output file dir. */
    private String outputFileDir = "src/test/resources/csv_output/";

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
        em = emf.createEntityManager();
        em.setProperty("kundera.fs.outputfile.path", outputFileDir);
        em.setProperty("format", "csv");
    }

    /**
     * Crud test.
     */
    @Test
    public void crudTest()
    {

        Person person = getPerson("1", "dev", 22, 50000.0);
        em.persist(person);

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
        person.setAge(age);
        person.setPersonId(id);
        person.setPersonName(name);
        person.setSalary(salary);
        return person;
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
        // SparkTestingUtils.recursivelyCleanDir(outputFileDir);
    }
}
