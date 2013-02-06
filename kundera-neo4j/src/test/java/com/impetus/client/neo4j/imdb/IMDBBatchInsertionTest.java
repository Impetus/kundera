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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;

/**
 * Test case demonstrating batch insertion in Neo4J 
 * @author amresh.singh
 */
public class IMDBBatchInsertionTest
{
    private static final String IMDB_PU = "imdb";
    EntityManagerFactory emf;
    EntityManager em; 
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {      
        /*Map properties = new HashMap();
        properties.put(PersistenceProperties.KUNDERA_BATCH_SIZE, "5");        
        emf = Persistence.createEntityManagerFactory(IMDB_PU, properties);        
        em = emf.createEntityManager();      */     
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
    public void batchTest()
    {
        /*List<Actor> actors = prepareData(4);        
        for(Actor actor : actors)
        {
            if(actor != null)
            {
                em.persist(actor);
            }
        }*/       
    }
    
    /**
     * n = number of Actors
     * @param n
     * @return
     */
    private List<Actor> prepareData(int n)
    {
        List<Actor> actors = new ArrayList<Actor>(); actors.add(0, null);
        List<Movie> movies = new ArrayList<Movie>(); movies.add(0, null);
        List<Role> roles = new ArrayList<Role>();    roles.add(0, null);
        
        
        for(int i = 1; i <= (n+1); i++)
        {
            Movie movie = new Movie("" + i, "Movie " + i, (2000+i));
            movies.add(i, movie);
        }
        
        for(int i = 1; i <= 2*n; i++)
        {
            Role role = new Role("Role " + i, "Role Type " + i);
            roles.add(i, role);
        }
        
        for(int i = 1; i <= n; i++)
        {
            Actor actor = new Actor(i, "Actor " + i);
            actors.add(i, actor);      
            
        } 
        
        for(int i = 1; i <=n; i++)
        {
            Actor actor = actors.get(i);
            actor.addMovie(roles.get(2*i -1), movies.get(i)); 
            actor.addMovie(roles.get(2 * i), movies.get(i + 1));           
            
            if(i == 1)
            {
                movies.get(i).addActor(roles.get(i), actor);
            }
            else
            {
                movies.get(i).addActor(roles.get(2*i - 2), actors.get(i - 1));
                movies.get(i).addActor(roles.get(2*i -1), actors.get(i));
            }            
        }
        movies.get(n + 1).addActor(roles.get(2 * n), actors.get(n));      
        
        return actors;
    }

}
