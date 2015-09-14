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

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.spark.entities.Person;
import com.impetus.client.spark.utils.SparkTestingUtils;

/**
 * The Class SparkCsvQueryTest.
 */
public class SparkCsvQueryTest extends SparkBaseTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The entity manager. */
    private EntityManager em;

    /** The pu. */
    private static final String PU = "spark_fs_pu";

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
        em.setProperty("kundera.fs.inputfile.path", "src/test/resources/test.csv");
        em.setProperty("format", "csv");
    }

    /**
     * Query test.
     */
    @Test
    public void queryTest()
    {
        List<Person> results = em.createNativeQuery("select * from spark_person").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        assertResults(results, true, true, true);

        results = em.createNativeQuery("select * from spark_person where salary > 35000").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        assertResults(results, false, true, true);

        results = em.createNativeQuery("select * from spark_person where salary > 35000 and age = 23").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertResults(results, false, true, false);

        results = em.createNativeQuery("select * from spark_person where personName like 'kp%'").getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        assertResults(results, false, false, true);

        List aggregateResults = em.createNativeQuery("select sum(salary) from spark_person").getResultList();
        Assert.assertNotNull(aggregateResults);

        aggregateResults = em.createNativeQuery("select count(*) from spark_person where salary > 30000")
                .getResultList();
        Assert.assertNotNull(aggregateResults.size());
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
        emf.close();
        SparkTestingUtils.recursivelyCleanDir(System.getProperty("user.dir")+"/metastore_db");
        emf = null;
        
    }
    
   
}
