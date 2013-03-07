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
package com.impetus.kundera.tests.crossdatastore.imdb.dao;

import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.junit.Assert;

import com.impetus.kundera.tests.crossdatastore.imdb.entities.Actor;
import com.impetus.kundera.tests.crossdatastore.imdb.entities.Movie;

/**
 * Implementation of {@link IMDBDao} 
 * @author amresh.singh
 */
public class IMDBDaoImpl extends BaseDao {
    
    private String persistenceUnit;

    /**
     * @param persistenceUnit
     */
    public IMDBDaoImpl(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }
    
    public void insert(Object actor)
    {
        em = getEntityManager(persistenceUnit);
        em.getTransaction().begin();
        em.persist(actor);
        em.getTransaction().commit();
        closeEntityManager();
    }    

    public Object find(Class entityClass, Object key)
    {
        em = getEntityManager(persistenceUnit);
        em.clear();
        Object actor = em.find(entityClass, key);
        return actor;
    }

    public void insertActors(Set<?> actors)
    {
        EntityManager em = getEntityManager(persistenceUnit);
        em.getTransaction().begin();
        for (Object actor : actors)
        {
            em.persist(actor);
        }
        em.getTransaction().commit();
        closeEntityManager();
    }

    public void remove(Object entity)
    {
        em = getEntityManager(persistenceUnit);       
        em.getTransaction().begin();
        em.remove(entity);
        em.getTransaction().commit();
        closeEntityManager();
    }

    public void merge(Object modifiedObj)
    {
        em = getEntityManager(persistenceUnit);
        em.getTransaction().begin();
        em.merge(modifiedObj);
        em.getTransaction().commit();
        closeEntityManager();
    } 
    

    public List<Actor> findAllActors()
    {
        em = getEntityManager(persistenceUnit);
        Query query = em.createQuery("select a from Actor a");
        List<Actor> actors = query.getResultList();        
        closeEntityManager();
        return actors;
    }

    public List<Actor> findActorByID()
    {
        em = getEntityManager(persistenceUnit);
        Query query = em.createQuery("select a from Actor a where a.id = :id");
        query.setParameter("id", 2);
        List<Actor> actors = query.getResultList();
        closeEntityManager();
        return actors;
    }

    public List<Actor> findActorByName()
    {
        em = getEntityManager(persistenceUnit);
        Query query = em.createQuery("select a from Actor a where a.name=:name");
        query.setParameter("name", "Tom Cruise");
        List<Actor> actors = query.getResultList();
        closeEntityManager();
        return actors;
    }

    public List<Actor> findActorByIDAndNamePositive()
    {
        em = getEntityManager(persistenceUnit);
        // Positive scenario
        Query query = em.createQuery("select a from Actor a where a.id=:id AND a.name=:name");
        query.setParameter("id", 1);
        query.setParameter("name", "Tom Cruise");
        List<Actor> actors = query.getResultList();
        closeEntityManager();
        return actors;        
    }
    
    public List<Actor> findActorByIDAndNameNegative()
    {
        em = getEntityManager(persistenceUnit);        // Negative scenario
        Query query = em.createQuery("select a from Actor a where a.id=:id AND a.name=:name");
        query.setParameter("id", 2);
        query.setParameter("name", "Tom Cruise");
        List<Actor> actors = query.getResultList();
        closeEntityManager();
        return actors;
    }

    public List<Actor> findActorWithMatchingName()
    {
        em = getEntityManager(persistenceUnit);
        Query query = em.createQuery("select a from Actor a where a.name like :name");
        query.setParameter("name", "Emma");
        List<Actor> actors = query.getResultList();
        closeEntityManager();
        return actors;
    }

    public List<Actor> findActorWithinGivenIdRange()
    {
        em = getEntityManager(persistenceUnit);
        
        Query query = em.createQuery("select a from Actor a where a.id between :min AND :max");
        query.setParameter("min", 1);
        query.setParameter("max", 2);
        List<Actor> actors = query.getResultList();
        closeEntityManager();
        return actors;
    }

    public List<Movie> findMoviesBetweenAPeriod()
    {
        em = getEntityManager(persistenceUnit);
        // Between
        Query query = em.createQuery("select m from Movie m where m.year between :start AND :end");
        query.setParameter("start", 1990);
        query.setParameter("end", 2006);
        List<Movie> movies = query.getResultList();
        closeEntityManager();
        return movies;

    }
    
    public List<Movie> findMoviesGreaterThanLessThanYear()
    {
        em = getEntityManager(persistenceUnit);
        // Greater-than/ Less Than
        Query query = em.createQuery("select m from Movie m where m.year >= :start AND m.year <= :end");
        query.setParameter("start", 2005);
        query.setParameter("end", 2010);
        List<Movie> movies = query.getResultList();
        closeEntityManager();
        return movies;
    }

    public List<Actor> findSelectedFields()
    {
        em = getEntityManager(persistenceUnit);
        Query query = em.createQuery("select a.name from Actor a");
        List<Actor> actors = query.getResultList();
        closeEntityManager();
        return actors;
    }

    public List<Movie> findMoviesUsingIdOrTitle()
    {
        em = getEntityManager(persistenceUnit);
        Query query = em.createQuery("select m from Movie m where m.id = :movieId OR m.title like :title");
        query.setParameter("movieId", "m1");
        query.setParameter("title", "Miss");
        List<Movie> movies = query.getResultList();
        closeEntityManager();
        return movies;
    }

    public List<Movie> findMoviesUsingIdOrTitleOrYear()
    {
        em = getEntityManager(persistenceUnit);
        Query query = em
                .createQuery("select m from Movie m where m.id = :movieId OR m.title like :title OR m.year = :year");
        query.setParameter("movieId", "m1");
        query.setParameter("title", "Miss");
        query.setParameter("year", 2009);
        List<Movie> movies = query.getResultList();
        closeEntityManager();
        return movies;
    }
    

}
