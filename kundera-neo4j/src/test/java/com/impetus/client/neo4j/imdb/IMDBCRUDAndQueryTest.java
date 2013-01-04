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
 * Test case using IMDB example for CRUD and query 
 * @author amresh.singh
 */
public class IMDBCRUDAndQueryTest
{
    EntityManagerFactory emf;
    EntityManager em;   
    


    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("imdb");
        em = emf.createEntityManager();        
        
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }  
    
    @Test
    public void testCRUD()
    {
        //Actors
        Actor actor1 = new Actor(1, "Tom Cruise");
        Actor actor2 = new Actor(2, "Emmanuelle Béart");     
        
        //Movies
        Movie movie1 = new Movie("m1", "War of the Worlds", 2005);
        Movie movie2 = new Movie("m2", "Mission Impossible", 1996);       
        Movie movie3 = new Movie("m3", "Hell", 2005);
        
        //Roles
        Role role1 = new Role("Ray Ferrier", "Lead Actor"); role1.setActor(actor1); role1.setMovie(movie1);
        Role role2 = new Role("Ethan Hunt", "Lead Actor"); role2.setActor(actor1); role2.setMovie(movie2);
        Role role3 = new Role("Claire Phelps", "Lead Actress"); role3.setActor(actor2); role1.setMovie(movie2);
        Role role4 = new Role("Sophie", "Supporting Actress"); role4.setActor(actor2); role1.setMovie(movie3);
        
        //Relationships
        actor1.addMovie(role1, movie1); actor1.addMovie(role2, movie2);
        actor2.addMovie(role3, movie2); actor2.addMovie(role4, movie3);
        
        movie1.addActor(role1, actor1);
        movie2.addActor(role2, actor1); movie2.addActor(role3, actor2);
        movie3.addActor(role4, actor2);
        
        em.persist(actor1);
        //em.persist(actor2);   
        
    }

}
