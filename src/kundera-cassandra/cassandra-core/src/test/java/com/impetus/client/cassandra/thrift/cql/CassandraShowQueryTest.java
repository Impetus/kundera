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
package com.impetus.client.cassandra.thrift.cql;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * test case for checking printed queries when kundera.show.query is enabled
 * 
 * @author shaheed.hussain
 * 
 */
public class CassandraShowQueryTest
{
    private static final String SHOW_QUERY_ENABLED_PU = "CassandraShowQueryEnabledPu";

    private static final String SHOW_QUERY_DISABLED_PU = "CassandraShowQueryDisabledPu";

    private EntityManagerFactory emf;

    private EntityManager em;

    private Map<String, Object> puProperties = new HashMap<String, Object>();

    private Logger logger = LoggerFactory.getLogger(CassandraShowQueryTest.class);

    @Before
    public void setUpBeforeClass() throws Exception
    {
        CassandraCli.cassandraSetUp();
        puProperties.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);

    }

    @After
    public void tearDownAfterClass() throws Exception
    {
        puProperties = null;

    }

    /*
     * testing show.query when it is enabled in persistence unit
     */
    @Test
    public void testShowQuerySetInPU()
    {
        emf = Persistence.createEntityManagerFactory(SHOW_QUERY_ENABLED_PU, puProperties);
        em = emf.createEntityManager();
        try
        {
            
            String expectedQuery[] = new String[3];
            String actualQuery = null;
            BufferedReader br = null;
            File file = new File("showQuery.log");
            PrintStream printStream;

            printStream = new PrintStream(new FileOutputStream(file));
            System.setOut(printStream);
            Query findQuery = em.createQuery("Select s from UserInformation s", UserInformation.class);
            findQuery.getResultList();
            expectedQuery[0] = "SELECT * FROM \"USER\" LIMIT 100";
            System.setOut(printStream);

            findQuery = em.createQuery("Select s from UserInformation s where s.name = vivek");
            findQuery.getResultList();
            expectedQuery[1] = "SELECT * FROM \"USER\" WHERE \"name\" = 'vivek' LIMIT 100  ALLOW FILTERING";
            System.setOut(printStream);

            findQuery = em.createQuery("Select s.age from UserInformation s where s.name = vivek");
            findQuery.getResultList();
            expectedQuery[2] = "SELECT \"age\" FROM \"USER\" WHERE \"name\" = 'vivek' LIMIT 100  ALLOW FILTERING";
            System.setOut(printStream);

            br = new BufferedReader(new FileReader("showQuery.log"));
            actualQuery = br.readLine();
            if (actualQuery == null)
                fail("failed as file is empty");

            while (actualQuery != null)
            {
                actualQuery.concat(br.readLine());
            }
            Assert.assertTrue(actualQuery.indexOf(expectedQuery[0]) != -1
                    && actualQuery.indexOf(expectedQuery[1]) != -1 && actualQuery.indexOf(expectedQuery[2]) != -1);
            em.close();
            emf.close();
        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }
    }

    /*
     * testing show.query property when it is disabled
     */
    @Test
    public void testShowQueryDisabled()
    {
        emf = Persistence.createEntityManagerFactory(SHOW_QUERY_DISABLED_PU, puProperties);
        em = emf.createEntityManager();
        try
        {
            String expectedQuery[] = new String[3];
            String actualQuery = null;
            boolean isFileEmpty = false;
            BufferedReader br = null;
            File file = new File("showQuery.log");
            PrintStream printStream;

            printStream = new PrintStream(new FileOutputStream(file));
            
            System.setOut(printStream);
            Query findQuery = em.createQuery("Select s from UserInformation s", UserInformation.class);
            findQuery.getResultList();
            expectedQuery[0] = "SELECT * FROM \"USER\" LIMIT 100";
            System.setOut(printStream);

            findQuery = em.createQuery("Select s from UserInformation s where s.name = vivek");
            findQuery.getResultList();
            expectedQuery[1] = "SELECT * FROM \"USER\" WHERE \"name\" = 'vivek' LIMIT 100  ALLOW FILTERING";
            System.setOut(printStream);

            findQuery = em.createQuery("Select s.age from UserInformation s where s.name = vivek");
            findQuery.getResultList();
            expectedQuery[2] = "SELECT \"age\" FROM \"USER\" WHERE \"name\" = 'vivek' LIMIT 100  ALLOW FILTERING";
            System.setOut(printStream);
            
            br = new BufferedReader(new FileReader("showQuery.log"));
            isFileEmpty = br.readLine() == null;
            
            while (actualQuery != null)
            {
                actualQuery.concat(br.readLine());
            }
            Assert.assertTrue(actualQuery.indexOf(expectedQuery[0]) == -1
                    && actualQuery.indexOf(expectedQuery[1]) == -1 && actualQuery.indexOf(expectedQuery[2]) == -1);
            
        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }
        em.close();
        emf.close();
    }

    /*
     * testing kunera.show.query property when it is enabled at external
     * properties level
     */

    @Test
    public void testShowQueryPropertySetInPropertyMap()
    {
        puProperties.put("kundera.show.query", "true");
        emf = Persistence.createEntityManagerFactory(SHOW_QUERY_DISABLED_PU, puProperties);
        em = emf.createEntityManager();
        try
        {
            
            String expectedQuery[] = new String[3];
            String actualQuery = null;
            BufferedReader br = null;
            File file = new File("showQuery.log");
            PrintStream printStream;

            printStream = new PrintStream(new FileOutputStream(file));
            System.setOut(printStream);
            Query findQuery = em.createQuery("Select s from UserInformation s", UserInformation.class);
            findQuery.getResultList();
            expectedQuery[0] = "SELECT * FROM \"USER\" LIMIT 100";
            System.setOut(printStream);

            findQuery = em.createQuery("Select s from UserInformation s where s.name = vivek");
            findQuery.getResultList();
            expectedQuery[1] = "SELECT * FROM \"USER\" WHERE \"name\" = 'vivek' LIMIT 100  ALLOW FILTERING";
            System.setOut(printStream);

            findQuery = em.createQuery("Select s.age from UserInformation s where s.name = vivek");
            findQuery.getResultList();
            expectedQuery[2] = "SELECT \"age\" FROM \"USER\" WHERE \"name\" = 'vivek' LIMIT 100  ALLOW FILTERING";
            System.setOut(printStream);

            br = new BufferedReader(new FileReader("showQuery.log"));
            actualQuery = br.readLine();
            if (actualQuery == null)
                fail("failed as file is empty");

            while (actualQuery != null)
            {
                actualQuery.concat(br.readLine());
            }
            Assert.assertTrue(actualQuery.indexOf(expectedQuery[0]) != -1
                    && actualQuery.indexOf(expectedQuery[1]) != -1 && actualQuery.indexOf(expectedQuery[2]) != -1);
            em.close();
            emf.close();
        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }
    }

}