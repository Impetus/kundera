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
 * Unit test case for Native queries (Lucene)
 * 
 * @author amresh.singh
 */
public class IMDBNativeLuceneQueryTest extends IMDBTestBase
{
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(IMDB_PU);
        em = emf.createEntityManager();

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

        em.close();
        emf.close();
    }

    @Test
    public void testLuceneQueries()
    {
        // Select Queries
        findAllActors();
        findActorByID();
        findActorByName();
        findActorByIDAndName();
        findActorWithMatchingName();
        findActorWithinGivenIdRange();
        findMoviesBetweenAPeriod();
        findMoviesGreaterThanLessThanYear();
        findMoviesUsingIdOrTitle();
        findMoviesUsingIdOrTitleOrYear();

    }

    private void findAllActors()
    {
        Query query = em.createNativeQuery("ACTOR_ID:*", Actor.class);
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(2, actors.size());
    }

    private void findActorByID()
    {
        Query query = em.createNativeQuery("ACTOR_ID:2", Actor.class);
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(1, actors.size());
        assertActor2(actors.get(0));
    }

    private void findActorByName()
    {
        Query query = em.createNativeQuery("ACTOR_NAME:\"Tom Cruise\"", Actor.class);
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(1, actors.size());
        assertActor1(actors.get(0));
    }

    private void findActorByIDAndName()
    {
        // Positive scenario
        Query query = em.createNativeQuery("ACTOR_ID:1 AND ACTOR_NAME:\"Tom Cruise\"", Actor.class);
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(1, actors.size());
        assertActor1(actors.get(0));

        // Negative scenario
        query = em.createNativeQuery("ACTOR_ID:2 AND ACTOR_NAME:\"Tom Cruise\"", Actor.class);
        actors = query.getResultList();
        Assert.assertTrue(actors == null || actors.isEmpty());
    }

    private void findActorWithMatchingName()
    {
        Query query = em.createNativeQuery("ACTOR_NAME:Emma*", Actor.class);
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(1, actors.size());
        assertActor2(actors.get(0));
    }

    private void findActorWithinGivenIdRange()
    {
        //Records are inclusive of range boundaries
        Query query = em.createNativeQuery("ACTOR_ID:[1 TO 2]", Actor.class); 
        List<Actor> actors = query.getResultList();
        Assert.assertNotNull(actors);
        Assert.assertFalse(actors.isEmpty());
        Assert.assertEquals(2, actors.size());
    }

    private void findMoviesBetweenAPeriod()
    {
        // Between
        //Records are exclusive of range boundaries
        Query query = em.createNativeQuery("YEAR:{1989 TO 2007}", Movie.class);       
        List<Movie> movies = query.getResultList();
        Assert.assertNotNull(movies);
        Assert.assertFalse(movies.isEmpty());
        Assert.assertEquals(2, movies.size());

    }

    private void findMoviesGreaterThanLessThanYear()
    {
        // Greater-than/ Less Than
        Query query = em.createNativeQuery("YEAR:[2005 TO 2010]", Movie.class);       
        List<Movie> movies = query.getResultList();
        Assert.assertNotNull(movies);
        Assert.assertFalse(movies.isEmpty());
        Assert.assertEquals(2, movies.size());
    }

    private void findMoviesUsingIdOrTitle()
    {
        Query query = em.createNativeQuery("MOVIE_ID:m1 OR TITLE:Miss*", Movie.class);        
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
        Query query = em.createNativeQuery("MOVIE_ID:m1 OR TITLE:Miss* OR YEAR:2009", Movie.class);        
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
}
