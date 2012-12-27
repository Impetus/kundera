/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.neo4j.imdb;


import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * IMDB Test case 
 * @author amresh.singh
 */
public class IMDBTest
{

    EntityManagerFactory emf;
    EntityManager em;   
    


    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        //emf = Persistence.createEntityManagerFactory("imdb");
        //em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        //em.close();
        //emf.close();
    }
    
    @Test
    public void testCRUD()
    {
        System.out.println("Running IMDB Test...");
    }

}
