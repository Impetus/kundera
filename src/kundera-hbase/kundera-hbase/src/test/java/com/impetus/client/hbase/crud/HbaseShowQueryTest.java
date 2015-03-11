/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.hbase.crud;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author shaheed.hussain
 * 
 */
public class HbaseShowQueryTest
{
    private static final String HBASE_PU = "hbaseTest";

    private EntityManagerFactory emf;

    private EntityManager em;

    private Map<String, String> puProperties = new HashMap<String, String>();

    private Logger logger = LoggerFactory.getLogger(HbaseShowQueryTest.class);

    private File file = null;

    @After
    public void tearDown() throws Exception
    {
        file.delete();
        em.close();
        emf.close();
        puProperties = null;
    }

    /*
     * testing show.query property when it is disabled
     */
    @Test
    public void testShowQueryDisabled() throws IOException
    {
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
        em = emf.createEntityManager();
        BufferedReader br = null;

        try
        {
            boolean isFileEmpty = false;
            file = new File("showQuery.log");
            PrintStream printStream;

            printStream = new PrintStream(new FileOutputStream(file));
            System.setOut(printStream);
            Query findQuery = em.createQuery("Select s from UserInfo s");
            findQuery.getResultList();
            System.setOut(printStream);

            findQuery = em.createQuery("Select p from UserInfo p where p.userId=\"PK_1\"");
            findQuery.getResultList();
            System.setOut(printStream);

            findQuery = em.createQuery("Select p from UserInfo p where p.userId=\"Shahid\"");
            findQuery.getResultList();
            System.setOut(printStream);

            br = new BufferedReader(new FileReader("showQuery.log"));
            isFileEmpty = br.readLine() == null;
            Assert.assertEquals(isFileEmpty, true);

        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }
        finally
        {
            br.close();
        }
    }

    /*
     * testing kunera.show.query property when it is enabled
     */
    @Test
    public void testShowQueryEnabled() throws IOException
    {
        puProperties.put("kundera.show.query", "true");
        emf = Persistence.createEntityManagerFactory(HBASE_PU, puProperties);
        em = emf.createEntityManager();
        BufferedReader br = null;
        try
        {
            int i = 0;
            String expectedQuery[] = new String[5];
            String actualQuery = null;
            file = new File("showQuery.log");
            PrintStream printStream;

            printStream = new PrintStream(new FileOutputStream(file));
            System.setOut(printStream);

            Query findQuery = em.createQuery("Select p from UserInfo p where p.userId=\"PK_1\"");
            findQuery.getResultList();
            expectedQuery[0] = "Fetch data from user_info for userId = [\"PK_1\"]";
            System.setOut(printStream);

            findQuery = em.createQuery("Select p from UserInfo p where p.userId=\"Shahid\"");
            findQuery.getResultList();
            expectedQuery[1] = "Fetch data from user_info for userId = [\"Shahid\"]";
            System.setOut(printStream);

            findQuery = em.createQuery("Select p from UserInfo p where p.age between 32 and 35");
            findQuery.getResultList();
            System.setOut(printStream);
            expectedQuery[2] = "Fetch data from user_info for age >= [32] AND age <= [35]";

            findQuery = em.createQuery("Select p from UserInfo p where p.userId=1 OR p.age=29");
            findQuery.getResultList();
            expectedQuery[3] = "Fetch data from user_info for userId = [1] OR age = [29]";
            System.setOut(printStream);

            findQuery = em.createQuery("Select p from UserInfo p where p.userId=1 AND p.age=32");
            findQuery.getResultList();
            System.setOut(printStream);
            expectedQuery[4] = "Fetch data from user_info for userId = [1] AND age = [32]";

            br = new BufferedReader(new FileReader("showQuery.log"));
            actualQuery = br.readLine();
            if (actualQuery == null)
                fail("failed as file is empty");

            while (actualQuery != null)
            {
                Assert.assertEquals(expectedQuery[i++], actualQuery);
                actualQuery = br.readLine();
            }
        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }
        finally
        {
            br.close();
        }

    }
}
