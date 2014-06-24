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
package com.impetus.client.mongodb.config;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
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
/**
 * test case for checking printed queries when kundera.show.query is enabled 
 * @author shaheed.hussain
 *
 */
public class MongoDBShowQueryTest
{

    private static final String SHOW_QUERY_ENABLED_PU = "mongoShowQueryEnabledPU";
    private static final String SHOW_QUERY_DISABLED_PU = "mongoShowQueryDisabledPU";

    private EntityManagerFactory emf;

    private EntityManager em;

    private Map<String, String> puProperties = new HashMap<String, String>();

    private Logger logger = LoggerFactory.getLogger(MongoDBShowQueryTest.class);

    @Before
    public void setUpBeforeClass() throws Exception
    {
      
        
        puProperties.put("kundera.keyspace", "showQueryKeyspace");
       
    }

    @After
    public void tearDownAfterClass() throws Exception
    {
        puProperties = null;
    }

    /*
     *testing show.query property when it is disabled
     */
    @Test
    public void testShowQueryDisabled() 
    {
        emf = Persistence.createEntityManagerFactory(SHOW_QUERY_DISABLED_PU, puProperties);
        em = emf.createEntityManager();
        try
        {

           
            boolean isFileEmpty=false;
            BufferedReader br = null;
            File file = new File("showQuery.log");
            PrintStream printStream;
            printStream = new PrintStream(new FileOutputStream(file));

            System.setOut(printStream);

            Query q = em.createQuery("Select p.personName from UserInformation p where p.personId = 2");
            q.getResultList();
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId < 3");
            q.getResultList();
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId >= 1 and p.personId < 3");
            q.getResultList();
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId = 1 and p.personName = vivek and p.age >=10 and p.age <= 20");
            q.getResultList();
            System.setOut(printStream);

            br = new BufferedReader(new FileReader("showQuery.log"));
            isFileEmpty=br.readLine() == null;
            Assert.assertEquals(isFileEmpty, true);
            em.close();
            emf.close();

        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }
      

    }
    
    /*
     * testing  show.query when it is enabled in persistence unit
     */
    @Test
    public void testShowQuerySetInPU() 
    {
        emf = Persistence.createEntityManagerFactory(SHOW_QUERY_ENABLED_PU, puProperties);
        em = emf.createEntityManager();
        try
        {

            int i = 0;
            String expectedQuery[] = new String[4];
            String actualQuery = null;
            BufferedReader br = null;
            File file = new File("showQuery.log");
            PrintStream printStream;
            printStream = new PrintStream(new FileOutputStream(file));

            System.setOut(printStream);

            Query q = em.createQuery("Select p.personName from UserInformation p where p.personId = 2");
            q.getResultList();
            expectedQuery[0] = "Find document: { \"_id\" : \"2\"}";
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId < 3");
            q.getResultList();
            expectedQuery[1] = "Find document: { \"_id\" : { \"$lt\" : \"3\"}}";
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId >= 1 and p.personId < 3");
            q.getResultList();
            expectedQuery[2] = "Find document: { \"_id\" : { \"$gte\" : \"1\" , \"$lt\" : \"3\"}}";
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId = 1 and p.personName = vivek and p.age >=10 and p.age <= 20");
            q.getResultList();
            expectedQuery[3] = "Find document: { \"_id\" : \"1\" , \"PERSON_NAME\" : \"vivek\" , \"AGE\" : { \"$gte\" : 10 , \"$lte\" : 20}}";
            System.setOut(printStream);

            br = new BufferedReader(new FileReader("showQuery.log"));
            actualQuery = br.readLine();
            if(actualQuery==null)
                fail("failed as file is empty");
           
            while (actualQuery  != null)
            {
                
                Assert.assertEquals(expectedQuery[i++], actualQuery);
                actualQuery = br.readLine();
            }
            em.close();
            emf.close();
        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }

    }
    
    /*
     * testing kunera.show.query property when it is enabled at external properties level
     */
    @Test
    public void testShowQuerySetInPropertyMap() 
    {
        puProperties.put("kundera.show.query","true");
        emf = Persistence.createEntityManagerFactory(SHOW_QUERY_DISABLED_PU, puProperties);
        em = emf.createEntityManager();
        try
        {

            int i = 0;
            String expectedQuery[] = new String[4];
            String actualQuery = null;
            BufferedReader br = null;
            File file = new File("showQuery.log");
            PrintStream printStream;
            printStream = new PrintStream(new FileOutputStream(file));

            System.setOut(printStream);

            Query q = em.createQuery("Select p.personName from UserInformation p where p.personId = 2");
            q.getResultList();
            expectedQuery[0] = "Find document: { \"_id\" : \"2\"}";
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId < 3");
            q.getResultList();
            expectedQuery[1] = "Find document: { \"_id\" : { \"$lt\" : \"3\"}}";
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId >= 1 and p.personId < 3");
            q.getResultList();
            expectedQuery[2] = "Find document: { \"_id\" : { \"$gte\" : \"1\" , \"$lt\" : \"3\"}}";
            System.setOut(printStream);

            q = em.createQuery("Select p.personName from UserInformation p where p.personId = 1 and p.personName = vivek and p.age >=10 and p.age <= 20");
            q.getResultList();
            expectedQuery[3] = "Find document: { \"_id\" : \"1\" , \"PERSON_NAME\" : \"vivek\" , \"AGE\" : { \"$gte\" : 10 , \"$lte\" : 20}}";
            System.setOut(printStream);

            br = new BufferedReader(new FileReader("showQuery.log"));
            
            actualQuery = br.readLine();
            if(actualQuery==null)
                fail("failed as file is empty");
           
            while (actualQuery  != null)
            {
                
                Assert.assertEquals(expectedQuery[i++], actualQuery);
                actualQuery = br.readLine();
            }
            em.close();
            emf.close();
        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }

    }
    
    
}
