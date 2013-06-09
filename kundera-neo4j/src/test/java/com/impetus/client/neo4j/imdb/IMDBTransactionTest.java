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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for validating transaction handling provided by Kundera for Neo4J
 * 
 * @author amresh.singh
 */
public class IMDBTransactionTest extends IMDBTestBase
{


    /*    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(IMDB_PU);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {

        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(IMDB_PU);
        String datastoreFilePath = puMetadata.getProperty(PersistenceProperties.KUNDERA_DATASTORE_FILE_PATH);
        FileUtils.deleteRecursively(new File(datastoreFilePath));
//        emf.close();
    }*/


    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {

        em.getTransaction().begin();
        em.remove(actor1);
        em.remove(actor2);
        em.getTransaction().commit();

//        em.close();
        clean();
    }

    @Test
    public void withTransaction()
    {
        try
        {
            /** Prepare data */
            // Actors
            populateActors();

            /** Insert records */
            em.getTransaction().begin();
            em.persist(actor1);
            em.persist(actor2);
            em.getTransaction().commit();

            /** Find records (Doesn't need transaction) */
            Actor actor11 = em.find(Actor.class, 1);
            Actor actor22 = em.find(Actor.class, 2);
            assertActors(actor11, actor22);

            /** Update records */
            em.clear();
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

            /** Delete records */
            em.clear();
            em.getTransaction().begin();
            em.remove(actor11);
            em.getTransaction().commit();
            em.getTransaction().begin();
            em.remove(actor22);
            em.getTransaction().commit();
            em.clear(); // clear cache
            Actor actor1AfterDeletion = em.find(Actor.class, 1);
            Actor actor2AfterDeletion = em.find(Actor.class, 2);
            Assert.assertNull(actor1AfterDeletion);
            Assert.assertNull(actor2AfterDeletion);

        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void withoutTransaction()
    {
        /** Prepare data */
        populateActors();

        /** Insert records without a transaction */
        try
        {
            em.persist(actor1);
            em.persist(actor2);
            Assert.fail();
        }
        catch (Exception e)
        {
            Assert.assertTrue(e.getMessage().toString().indexOf("transaction") >= 0);
        }

        /** Find records */
        em.clear();
        Actor actor11 = em.find(Actor.class, 1);
        Actor actor22 = em.find(Actor.class, 2);
        Assert.assertNull(actor11);
        Assert.assertNull(actor22);

    }

    @Test
    public void rollbackBehavior()
    {
        populateActors();

        em.getTransaction().begin();
        em.persist(actor1);
        em.persist(actor2);
        em.getTransaction().rollback();

        em.clear();
        Actor actor11 = em.find(Actor.class, 1);
        Actor actor22 = em.find(Actor.class, 2);
        Assert.assertNull(actor11);
        Assert.assertNull(actor22);
    }

    @Test
    public void rollbackBehaviorOnException()
    {
        populateActors();

        try
        {
            em.getTransaction().begin();
            em.persist(actor1);
            em.persist(actor2);
            em.getTransaction().commit();

            em.getTransaction().begin();
            actor1.setName("Amresh");
            actor2.setName("Amir");
            em.merge(actor1);
            em.merge(actor2);
            em.merge(null);
            em.getTransaction().commit();
        }
        catch (Exception e)
        {
            em.clear();
            Actor actor11 = em.find(Actor.class, 1);
            Actor actor22 = em.find(Actor.class, 2);
            Assert.assertNotNull(actor11);
            Assert.assertNotNull(actor22);
            Assert.assertNotSame("Amresh", actor11.getName());
            Assert.assertNotSame("Amir", actor22.getName());

        }

    }
}
