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
import java.util.List;

import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * * Test case using IMDB example for JPA Queries Demonstrates M-2-M Association
 * between two entitites using Map
 * 
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
        init();

        // Prepare and insert data
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
        // Delete inserted records
        Actor actor1 = em.find(Actor.class, 1);
        Actor actor2 = em.find(Actor.class, 2);

        if (actor1 != null && actor2 != null)
        {
            em.getTransaction().begin();
            em.remove(actor1);
            em.remove(actor2);
            em.getTransaction().commit();
        }

        clean();
    }

    @Test
    public void testJPAQueries()
    {
        // Select Queries
        findAllActors();
        findActorByID();
        findActorByName();
        findActorByIDAndName();
        findActorUsingInClause();
        findActorWithMatchingName();
        findActorWithinGivenIdRange();
        findSelectedFields();
        findMoviesBetweenAPeriod();
        findMoviesGreaterThanLessThanYear();
        findMoviesUsingIdOrTitle();
        findMoviesUsingIdOrTitleOrYear();
        // Delete Queries
        deleteAllActors();

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

    private void findActorByIDAndName()
    {
        // Positive scenario
        Query query = em.createQuery("select a from Actor a where a.id=:id AND a.name=:name");
        query.setParameter("id", 1);
        query.setParameter("name", "Tom Cruise");
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(1, actors.size());
        assertActor1(actors.get(0));

        // Negative scenario
        query = em.createQuery("select a from Actor a where a.id=:id AND a.name=:name");
        query.setParameter("id", 2);
        query.setParameter("name", "Tom Cruise");
        actors = query.getResultList();
        Assert.assertTrue(actors == null || actors.isEmpty());
    }

    private void findActorUsingInClause() {
        	Query query = em.createQuery("select a from Actor a where a.name in ('Tom Cruise','Emmanuelle Béart')");
        	List<Actor> actors = query.getResultList();
        	Assert.assertNotNull(actors);
            Assert.assertFalse(actors.isEmpty());
            Assert.assertEquals(2, actors.size());
            
            query = em.createQuery("select a from Actor a where a.name in ('Tom Cruise','Brad Pitt')");
        	actors = query.getResultList();
        	Assert.assertNotNull(actors);
            Assert.assertFalse(actors.isEmpty());
            Assert.assertEquals(1, actors.size());
            
            query = em.createQuery("select a from Actor a where a.name in :names");
            List<String> names = new ArrayList<String>();
            names.add("Tom Cruise");
            names.add("Brad Pitt");
            query.setParameter("names", names);
        	actors = query.getResultList();
        	Assert.assertNotNull(actors);
            Assert.assertFalse(actors.isEmpty());
            Assert.assertEquals(1, actors.size());
    		
            query = em.createQuery("select a from Actor a where a.name in :names");
            String[] namesArray = new String[]{"Tom Cruise","Brad Pitt"};
            query.setParameter("names", namesArray);
        	actors = query.getResultList();
        	Assert.assertNotNull(actors);
            Assert.assertFalse(actors.isEmpty());
            Assert.assertEquals(1, actors.size());
    	}

    private void findActorWithMatchingName()
    {
        Query query = em.createQuery("select a from Actor a where a.name like :name");
        query.setParameter("name", "Emma");
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(1, actors.size());
        assertActor2(actors.get(0));
    }

    private void findActorWithinGivenIdRange()
    {
        Query query = em.createQuery("select a from Actor a where a.id between :min AND :max");
        query.setParameter("min", 1);
        query.setParameter("max", 2);
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(2, actors.size());
        // assertActor2(actors.get(0));
    }

    private void findMoviesBetweenAPeriod()
    {
        // Between
        Query query = em.createQuery("select m from Movie m where m.year between :start AND :end");
        query.setParameter("start", 1990);
        query.setParameter("end", 2006);
        List<Movie> movies = query.getResultList();
        Assert.assertNotNull(movies);
        Assert.assertFalse(movies.isEmpty());
        Assert.assertEquals(2, movies.size());

    }

    private void findMoviesGreaterThanLessThanYear()
    {
        // Greater-than/ Less Than
        Query query = em.createQuery("select m from Movie m where m.year >= :start AND m.year <= :end");
        query.setParameter("start", 2005);
        query.setParameter("end", 2010);
        List<Movie> movies = query.getResultList();
        Assert.assertNotNull(movies);
        Assert.assertFalse(movies.isEmpty());
        Assert.assertEquals(2, movies.size());
    }

    private void findSelectedFields()
    {
        Query query = em.createQuery("select a.name from Actor a");
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(2, actors.size());

        for (Actor actor : actors)
        {
            Assert.assertNotNull(actor);
            Assert.assertNotNull(actor.getId());
            Assert.assertNotNull(actor.getName());
        }
    }

    private void findMoviesUsingIdOrTitle()
    {
        Query query = em.createQuery("select m from Movie m where m.id = :movieId OR m.title like :title");
        query.setParameter("movieId", "m1");
        query.setParameter("title", "Miss");
        List<Movie> movies = query.getResultList();
        Assert.assertNotNull(movies);
        Assert.assertFalse(movies.isEmpty());
        Assert.assertEquals(2, movies.size());

        for (Movie movie : movies)
        {
            Assert.assertNotNull(movie);
            Assert.assertTrue(movie.getId().equals("m1") || movie.getId().equals("m2"));
        }
    }

    private void findMoviesUsingIdOrTitleOrYear()
    {
        Query query = em
                .createQuery("select m from Movie m where m.id = :movieId OR m.title like :title OR m.year = :year");
        query.setParameter("movieId", "m1");
        query.setParameter("title", "Miss");
        query.setParameter("year", 2009);
        List<Movie> movies = query.getResultList();
        Assert.assertNotNull(movies);
        Assert.assertFalse(movies.isEmpty());
        Assert.assertEquals(3, movies.size());

        for (Movie movie : movies)
        {
            Assert.assertNotNull(movie);
            Assert.assertTrue(movie.getId().equals("m1") || movie.getId().equals("m2") || movie.getId().equals("m3"));
        }
    }

    private void deleteAllActors()
    {
        em.getTransaction().begin();
        Query query = em.createQuery("delete from Actor a");
        int deleteCount = query.executeUpdate();
        em.getTransaction().commit();
        Assert.assertEquals(2, deleteCount);

        // Check whether all records have been deleted
        query = em.createQuery("select a from Actor a");
        List<Actor> actors = query.getResultList();
        Assert.assertTrue(actors == null || actors.isEmpty());
    }

}
