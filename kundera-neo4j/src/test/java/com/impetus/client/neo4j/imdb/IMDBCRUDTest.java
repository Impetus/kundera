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

import java.io.File;

import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Test case using IMDB example for CRUD Demonstrates M-2-M Association between
 * two entitites using Map
 * 
 * @author amresh.singh
 */
public class IMDBCRUDTest extends IMDBTestBase
{
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        init();
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
        // CRUD
        insert();
        find();
        merge();
        delete();
    }

    private void insert()
    {
        populateActors();

        em.getTransaction().begin();

        em.persist(actor1);
        em.persist(actor2);

        em.getTransaction().commit();

    }

    private void find()
    {
        // Find actor by ID
        em.clear();

        Actor actor1 = em.find(Actor.class, 1);
        Actor actor2 = em.find(Actor.class, 2);

        assertActors(actor1, actor2);

    }

    private void merge()
    {
        Actor actor1 = em.find(Actor.class, 1);
        Actor actor2 = em.find(Actor.class, 2);

        assertActors(actor1, actor2);

        actor1.setName("Amresh");
        actor2.setName("Amir");

        em.getTransaction().begin();
        em.merge(actor1);
        em.merge(actor2);
        em.getTransaction().commit();

        em.clear();

        Actor actor1AfterMerge = em.find(Actor.class, 1);
        Actor actor2AfterMerge = em.find(Actor.class, 2);

        assertUpdatedActors(actor1AfterMerge, actor2AfterMerge);

    }

    private void delete()
    {
        Actor actor1 = em.find(Actor.class, 1);
        Actor actor2 = em.find(Actor.class, 2);
        assertUpdatedActors(actor1, actor2);

        em.getTransaction().begin();
        em.remove(actor1);
        em.remove(actor2);
        em.getTransaction().commit();

        em.clear(); // clear cache
        Actor actor1AfterDeletion = em.find(Actor.class, 1);
        Actor actor2AfterDeletion = em.find(Actor.class, 2);

        Assert.assertNull(actor1AfterDeletion);
        Assert.assertNull(actor2AfterDeletion);

        Movie movie1AfterDeletion = em.find(Movie.class, "m1");
        Movie movie2AfterDeletion = em.find(Movie.class, "m2");
        Movie movie3AfterDeletion = em.find(Movie.class, "m3");

        Assert.assertNull(movie1AfterDeletion);
        Assert.assertNull(movie2AfterDeletion);
        Assert.assertNull(movie3AfterDeletion);

    }

}
