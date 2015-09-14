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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.spark.entities.Person;
import com.impetus.client.spark.utils.SparkTestingUtils;

/**
 * The Class SparkHDFSClientTest.
 * 
 * @author amitkumar
 */
public class SparkHDFSClientTest extends SparkBaseTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The entity manager. */
    private EntityManager em;

    /** The pu. */
    private static final String PU = "spark_hdfs_pu";

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
        em.setProperty("kundera.hdfs.outputfile.path", "hdfs://localhost:9000/sparkInputTest/input");
        em.setProperty("kundera.hdfs.inputfile.path", "hdfs://localhost:9000/sparkInputTest/input");
        em.setProperty("format", "json");
    }

    /**
     * Spark hdfs test.
     */
    @Test
    public void sparkHdfsTest()
    {
        testPersist();
        testQuery();
        testSaveIntermediateResult();
    }

    public void testPersist()
    {
        Person person1 = getPerson("1", "dev", 22, 30000.5);

        em.persist(person1);
        em.clear();
        Person p = em.find(Person.class, "1");
        validatePerson1(p);
    }

    /**
     * Test query.
     */
    public void testQuery()
    {
        List<Person> results = em.createNativeQuery("select * from spark_person").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        validatePerson1(results.get(0));
    }

    /**
     * Test save intermediate result.
     */
    public void testSaveIntermediateResult()
    {
        String sqlString = "INSERT INTO fs.[src/test/resources/testspark_csv] AS CSV FROM (select * from spark_person)";
        Query q = em.createNativeQuery(sqlString, Person.class);
        q.executeUpdate();

        sqlString = "INSERT INTO fs.[src/test/resources/testspark_json] AS JSON FROM (select * from spark_person)";
        q = em.createNativeQuery(sqlString, Person.class);
        q.executeUpdate();
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
        em.close();
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
        SparkTestingUtils.recursivelyCleanDir("src/test/resources/testspark_json");
        SparkTestingUtils.recursivelyCleanDir("src/test/resources/testspark_csv");
        emf.close();
        SparkTestingUtils.recursivelyCleanDir(System.getProperty("user.dir")+"/metastore_db");
        emf = null;
    }

}
