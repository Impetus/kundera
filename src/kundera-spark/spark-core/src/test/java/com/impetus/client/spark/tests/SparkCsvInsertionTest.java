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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.spark.entities.Person;
import com.impetus.client.spark.utils.SparkTestingUtils;

/**
 * The Class SparkCsvCrudTest.
 */
public class SparkCsvInsertionTest extends SparkBaseTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The entity manager. */
    private EntityManager em;

    /** The Constant PU. */
    private static final String PU = "spark_fs_pu";

    /** The Constant OUTPUT_FILE_DIR. */
    private static final String OUTPUT_FILE_DIR = "src/test/resources/csv_output/";

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
        em.setProperty("kundera.fs.outputfile.path", OUTPUT_FILE_DIR);
        em.setProperty("format", "csv");
    }

    /**
     * Crud test.
     */
    @Test
    public void testInsert()
    {
        Person person1 = getPerson("1", "dev", 22, 30000.5);
        em.persist(person1);
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
        SparkTestingUtils.recursivelyCleanDir(OUTPUT_FILE_DIR);
        emf.close();
        SparkTestingUtils.recursivelyCleanDir(System.getProperty("user.dir")+"/metastore_db");
        emf = null;
    }
}
