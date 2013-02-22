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

import java.util.List;

import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * * Test case using IMDB example for JPA Queries
 * Demonstrates M-2-M Association between two entitites using Map
 * @author amresh.singh
 */
public class IMDBJPAQueriesTest extends IMDBTestBase
{
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {           
        emf = Persistence.createEntityManagerFactory(IMDB_PU);      
        em = emf.createEntityManager();
        
        //Prepare and insert data
        populateActors();
        em.getTransaction().begin();        
        em.persist(actor1);
        em.persist(actor2);        
        em.getTransaction().commit();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {   
        //Delete inserted records
        Actor actor1 = em.find(Actor.class, 1);
        Actor actor2 = em.find(Actor.class, 2);    
        
        em.getTransaction().begin();
        em.remove(actor1);
        em.remove(actor2);
        em.getTransaction().commit();
        
        em.close();     
        emf.close();      
    } 
    
    @Test
    public void testJPAQueries()
    {
        //Queries
        findAllActors();
        findActorByID();
        findActorByName();
    }
    
    
    private void findAllActors()
    {
        Query query = em.createQuery("select a from Actor a");
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(2, actors.size());      
    }
    
    private void findActorByID()
    {
        Query query = em.createQuery("select a from Actor a where a.id = :id");
        query.setParameter("id", 2);
        List<Actor> actors = query.getResultList();        
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(1, actors.size());        
        assertActor2(actors.get(0));        
    }
    
    private void findActorByName()
    {
        Query query = em.createQuery("select a from Actor a where a.name=:name");
        query.setParameter("name", "Tom Cruise");
        List<Actor> actors = query.getResultList();        
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(1, actors.size());        
        assertActor1(actors.get(0));        
    }    

}
