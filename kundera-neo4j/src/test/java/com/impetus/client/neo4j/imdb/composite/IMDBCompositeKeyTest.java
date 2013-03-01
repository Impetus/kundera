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
package com.impetus.client.neo4j.imdb.composite;

import java.io.File;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Test case for entities that hold composite keys  
 * @author amresh.singh
 */
public class IMDBCompositeKeyTest
{
    
    EntityManagerFactory emf;
    EntityManager em;   
    
    private static final String IMDB_PU = "imdb";
    
    ActorComposite actor1;
    ActorComposite actor2;  


    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {           
    	KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        emf = Persistence.createEntityManagerFactory(IMDB_PU);      
        em = emf.createEntityManager();       
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {   
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(IMDB_PU);
        String datastoreFilePath = puMetadata.getProperty(PersistenceProperties.KUNDERA_DATASTORE_FILE_PATH);
        
        em.close();
        emf.close();
           
    }  
    
    @Test
    public void testCompositeKeys()
    {
        insert();    
        findById();
        merge();
        delete();
    }   

    /**
     * 
     */
    public void insert()
    {
        prepareData();
        em.getTransaction().begin();
        em.persist(actor1);
        em.persist(actor2);
        em.getTransaction().commit();
    }
    
    private void findById()
    {
        //Find actor by ID
        em.clear();        
        ActorComposite actor1 = em.find(ActorComposite.class, new ActorId("A", 1));
        ActorComposite actor2 = em.find(ActorComposite.class, new ActorId("A", 2));    
        
        
        assertActors(actor1, actor2);          
    }
    
    private void merge()
    {
        em.clear();        
        ActorComposite actor1 = em.find(ActorComposite.class, new ActorId("A", 1));
        ActorComposite actor2 = em.find(ActorComposite.class, new ActorId("A", 2));         
        assertActors(actor1, actor2);  
        
        actor1.setName("Amresh");
        actor2.setName("Amir");
        
        em.getTransaction().begin();
        em.merge(actor1);
        em.merge(actor2);
        em.getTransaction().commit();
        
        em.clear();
        
        ActorComposite actor1AfterMerge = em.find(ActorComposite.class, new ActorId("A", 1));
        ActorComposite actor2AfterMerge = em.find(ActorComposite.class, new ActorId("A", 2));    
        
        assertUpdatedActors(actor1AfterMerge, actor2AfterMerge);
    }
    
    private void delete()
    {
        em.clear();
        ActorComposite actor1 = em.find(ActorComposite.class, new ActorId("A", 1));
        ActorComposite actor2 = em.find(ActorComposite.class, new ActorId("A", 2));
        
        
        em.getTransaction().begin();
        em.remove(actor1);
        em.remove(actor2);
        em.getTransaction().commit();
        
        em.clear();
        ActorComposite actor1AfterDeletion = em.find(ActorComposite.class, new ActorId("A", 1));
        ActorComposite actor2AfterDeletion = em.find(ActorComposite.class, new ActorId("A", 2));
        Assert.assertNull(actor1AfterDeletion);
        Assert.assertNull(actor2AfterDeletion);
    }
    
    private void prepareData()
    {
        //Actors
        actor1 = new ActorComposite(new ActorId("A", 1), "Tom Cruise");
        actor2 = new ActorComposite(new ActorId("A", 2), "Emmanuelle Béart");     
        
        //Movies
        MovieComposite movie1 = new MovieComposite(new MovieId('U', 11111111L), "War of the Worlds", 2005);
        MovieComposite movie2 = new MovieComposite(new MovieId('U', 22222222L), "Mission Impossible", 1996);       
        MovieComposite movie3 = new MovieComposite(new MovieId('A', 33333333L), "Hell", 2005);
        
        //Roles
        RoleComposite role1 = new RoleComposite(new RoleId("Ray", "Ferrier"), "Lead Actor"); role1.setActor(actor1); role1.setMovie(movie1);
        RoleComposite role2 = new RoleComposite(new RoleId("Ethan", "Hunt"), "Lead Actor"); role2.setActor(actor1); role2.setMovie(movie2);
        RoleComposite role3 = new RoleComposite(new RoleId("Claire", "Phelps"), "Lead Actress"); role3.setActor(actor2); role1.setMovie(movie2);
        RoleComposite role4 = new RoleComposite(new RoleId("Sophie", ""), "Supporting Actress"); role4.setActor(actor2); role1.setMovie(movie3);
        
        //Relationships
        actor1.addMovie(role1, movie1); actor1.addMovie(role2, movie2);
        actor2.addMovie(role3, movie2); actor2.addMovie(role4, movie3);
        
        movie1.addActor(role1, actor1);
        movie2.addActor(role2, actor1); movie2.addActor(role3, actor2);
        movie3.addActor(role4, actor2);
    }
    
    private void assertActors(ActorComposite actor1, ActorComposite actor2)
    {
        Assert.assertNotNull(actor1);
        Assert.assertEquals("A", actor1.getActorId().getPrefix());
        Assert.assertEquals(1, actor1.getActorId().getSuffix());
        Assert.assertEquals("Tom Cruise", actor1.getName());
        Map<RoleComposite, MovieComposite> movies1 = actor1.getMovies();
        Assert.assertFalse(movies1 == null || movies1.isEmpty());
        Assert.assertEquals(2, movies1.size());
        
        
        Assert.assertNotNull(actor2);
        Assert.assertEquals("A", actor2.getActorId().getPrefix());
        Assert.assertEquals(2, actor2.getActorId().getSuffix());
        Assert.assertEquals("Emmanuelle Béart", actor2.getName());
        Map<RoleComposite, MovieComposite> movies2 = actor2.getMovies();
        Assert.assertFalse(movies2 == null || movies2.isEmpty());
        Assert.assertEquals(2, movies2.size());
    }
    
    private void assertUpdatedActors(ActorComposite actor1, ActorComposite actor2)
    {
        Assert.assertNotNull(actor1);
        Assert.assertEquals("A", actor1.getActorId().getPrefix());
        Assert.assertEquals(1, actor1.getActorId().getSuffix());
        Assert.assertEquals("Amresh", actor1.getName());
        Map<RoleComposite, MovieComposite> movies1 = actor1.getMovies();
        Assert.assertFalse(movies1 == null || movies1.isEmpty());
        Assert.assertEquals(2, movies1.size());
        
        
        Assert.assertNotNull(actor2);
        Assert.assertEquals("A", actor2.getActorId().getPrefix());
        Assert.assertEquals(2, actor2.getActorId().getSuffix());
        Assert.assertEquals("Amir", actor2.getName());
        Map<RoleComposite, MovieComposite> movies2 = actor2.getMovies();
        Assert.assertFalse(movies2 == null || movies2.isEmpty());
        Assert.assertEquals(2, movies2.size());
    }

}
