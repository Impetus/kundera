package com.impetus.client.neo4j.imdb;

import java.io.File;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * @author impadmin
 * 
 */
public class ActorWithMultipleRelationTest
{
    private static final String IMDB_PU = "imdb";

    private EntityManagerFactory emf;

    private KunderaMetadata kunderaMetadata;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(IMDB_PU);
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        PersistenceUnitMetadata puMetadata = KunderaMetadataManager
                .getPersistenceUnitMetadata(kunderaMetadata, IMDB_PU);
        String datastoreFilePath = puMetadata.getProperty(PersistenceProperties.KUNDERA_DATASTORE_FILE_PATH);
        emf.close();
        if (datastoreFilePath != null)
        {
            FileUtils.deleteRecursively(new File(datastoreFilePath));
        }
    }

    @Test
    public void test()
    {
        // Actors
        ActorWithMultipleRelation actor = new ActorWithMultipleRelation(1, "Tom Cruise");

        // Latest Movies
        LatestMovie latestMovie1 = new LatestMovie("m1", "War of the Worlds", 2005);
        LatestMovie latestMovie2 = new LatestMovie("m2", "Mission Impossible", 1996);

        // New Roles
        NewRole newRole1 = new NewRole("Ray Ferrier", "Lead Actor");
        newRole1.setActor(actor);
        newRole1.setMovie(latestMovie1);

        NewRole newRole2 = new NewRole("Ethan Hunt", "Lead Actor");
        newRole2.setActor(actor);
        newRole2.setMovie(latestMovie2);

        // Relationships
        actor.addLatestMovie(newRole1, latestMovie1);
        actor.addLatestMovie(newRole2, latestMovie2);

        // latestMovie1.addActor(newRole1, actor);

        EntityManager em = emf.createEntityManager();

        em.getTransaction().begin();
        em.persist(actor);
        em.getTransaction().commit();
        em.clear();

        ActorWithMultipleRelation result = em.find(ActorWithMultipleRelation.class, 1);
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getName());
        Assert.assertNotNull(result.getId());
        Assert.assertNotNull(result.getLatestMovies());
        Assert.assertFalse(result.getLatestMovies().isEmpty());
        Assert.assertNotNull(result.getLatestMovies().get(newRole1));
        Assert.assertNotNull(result.getLatestMovies().get(newRole2));
        Assert.assertNotNull(result.getArchivedMovies());
        Assert.assertTrue(result.getArchivedMovies().isEmpty());

        List<ActorWithMultipleRelation> results = em.createQuery("Select a from ActorWithMultipleRelation a")
                .getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertNotNull(results.get(0));

        result = results.get(0);

        Assert.assertNotNull(result.getName());
        Assert.assertNotNull(result.getId());
        Assert.assertNotNull(result.getLatestMovies());
        Assert.assertFalse(result.getLatestMovies().isEmpty());
        Assert.assertNotNull(result.getLatestMovies().get(newRole1));
        Assert.assertNotNull(result.getLatestMovies().get(newRole2));
        Assert.assertNotNull(result.getArchivedMovies());
        Assert.assertTrue(result.getArchivedMovies().isEmpty());

        // Remove
        em.getTransaction().begin();
        em.remove(result);
        em.getTransaction().commit();
        result = em.find(ActorWithMultipleRelation.class, 1);
        Assert.assertNull(result);

        // Adding one more relationship to actor.
        // Latest Movies
        ArchivedMovie archivedMovie1 = new ArchivedMovie("m1", "War of the Worlds", 2005);
        ArchivedMovie archivedMovie2 = new ArchivedMovie("m2", "Mission Impossible", 1996);

        // New Roles
        OldRole oldRole1 = new OldRole("Ray Ferrier", "Lead Actor");
        oldRole1.setActor(actor);
        oldRole1.setMovie(archivedMovie1);

        OldRole oldRole2 = new OldRole("Ethan Hunt", "Lead Actor");
        oldRole2.setActor(actor);
        oldRole2.setMovie(archivedMovie2);

        // Relationships
        actor.addArchivedMovie(oldRole1, archivedMovie1);
        actor.addArchivedMovie(oldRole2, archivedMovie2);

        // archivedMovie2.addActor(oldRole1, actor);

        em.getTransaction().begin();
        em.persist(actor);
        em.getTransaction().commit();
        em.clear();

        result = em.find(ActorWithMultipleRelation.class, 1);
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getName());
        Assert.assertNotNull(result.getId());
        Assert.assertNotNull(result.getLatestMovies());
        Assert.assertFalse(result.getLatestMovies().isEmpty());
        Assert.assertNotNull(result.getLatestMovies().get(newRole1));
        Assert.assertNotNull(result.getLatestMovies().get(newRole2));
        Assert.assertNotNull(result.getArchivedMovies());
        Assert.assertFalse(result.getArchivedMovies().isEmpty());
        Assert.assertNotNull(result.getArchivedMovies().get(oldRole1));
        Assert.assertNotNull(result.getArchivedMovies().get(oldRole2));

        results = em.createQuery("Select a from ActorWithMultipleRelation a").getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertNotNull(results.get(0));

        result = results.get(0);

        Assert.assertNotNull(result.getName());
        Assert.assertNotNull(result.getId());
        Assert.assertNotNull(result.getLatestMovies());
        Assert.assertFalse(result.getLatestMovies().isEmpty());
        Assert.assertNotNull(result.getLatestMovies().get(newRole1));
        Assert.assertNotNull(result.getLatestMovies().get(newRole2));
        Assert.assertNotNull(result.getArchivedMovies());
        Assert.assertFalse(result.getArchivedMovies().isEmpty());
        Assert.assertNotNull(result.getArchivedMovies().get(oldRole1));
        Assert.assertNotNull(result.getArchivedMovies().get(oldRole2));

        // Remove
        em.getTransaction().begin();
        em.remove(result);
        em.getTransaction().commit();
        result = em.find(ActorWithMultipleRelation.class, 1);
        Assert.assertNull(result);

    }

}
