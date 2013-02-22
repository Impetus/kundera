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

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.junit.Assert;

/**
 * Base class for All IMDB Tests
 * @author amresh.singh
 */
public class IMDBTestBase
{
    Actor actor1;
    Actor actor2;
    
    EntityManagerFactory emf;
    EntityManager em;   
    
    protected static final String IMDB_PU = "imdb";  
    
    /**
     * @param actor1
     * @param actor2
     */
    protected void assertActors(Actor actor1, Actor actor2)
    {
        assertActor1(actor1);       
        assertActor2(actor2);
    }

    /**
     * @param actor2
     */
    protected void assertActor2(Actor actor2)
    {
        Assert.assertNotNull(actor2);
        Assert.assertEquals(2, actor2.getId());
        Assert.assertEquals("Emmanuelle Béart", actor2.getName());
        Map<Role, Movie> movies2 = actor2.getMovies();
        Assert.assertFalse(movies2 == null || movies2.isEmpty());
        Assert.assertEquals(2, movies2.size());
    }

    /**
     * @param actor1
     */
    protected void assertActor1(Actor actor1)
    {
        Assert.assertNotNull(actor1);
        Assert.assertEquals(1, actor1.getId());
        Assert.assertEquals("Tom Cruise", actor1.getName());
        Map<Role, Movie> movies1 = actor1.getMovies();
        Assert.assertFalse(movies1 == null || movies1.isEmpty());
        Assert.assertEquals(2, movies1.size());
    }
    
    /**
     * @param actor1
     * @param actor2
     */
    protected void assertUpdatedActors(Actor actor1, Actor actor2)
    {
        assertUpdatedActor1(actor1);       
        assertUpdatedActor2(actor2);
    }

    /**
     * @param actor2
     */
    protected void assertUpdatedActor2(Actor actor2)
    {
        Assert.assertNotNull(actor2);
        Assert.assertEquals(2, actor2.getId());
        Assert.assertEquals("Amir", actor2.getName());
        Map<Role, Movie> movies2 = actor2.getMovies();
        Assert.assertFalse(movies2 == null || movies2.isEmpty());
        Assert.assertEquals(2, movies2.size());
    }

    /**
     * @param actor1
     */
    protected void assertUpdatedActor1(Actor actor1)
    {
        Assert.assertNotNull(actor1);
        Assert.assertEquals(1, actor1.getId());
        Assert.assertEquals("Amresh", actor1.getName());
        Map<Role, Movie> movies1 = actor1.getMovies();
        Assert.assertFalse(movies1 == null || movies1.isEmpty());
        Assert.assertEquals(2, movies1.size());
    }  
    
    protected void populateActors()
    {
        //Actors
        actor1 = new Actor(1, "Tom Cruise");
        actor2 = new Actor(2, "Emmanuelle Béart");     
        
        //Movies
        Movie movie1 = new Movie("m1", "War of the Worlds", 2005);
        Movie movie2 = new Movie("m2", "Mission Impossible", 1996);       
        Movie movie3 = new Movie("m3", "Hell", 2009);
        
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
    }

}
