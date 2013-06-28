package com.impetus.kundera.tests.persistence.lazy;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceUnitUtil;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KunderaPersistenceUnitUtilTest
{
    EntityManagerFactory emf;

    EntityManager em;

    PersistenceUnitUtil util;

    LazyTestSetup setup = new LazyTestSetup();

    @Before
    public void setUp() throws Exception
    {
        setup.startServer();
        setup.createSchema();

        emf = Persistence
                .createEntityManagerFactory("rdbms,redis,addMongo,addCassandra,piccandra,secIdxAddCassandra,picongo");
        em = emf.createEntityManager();
        util = emf.getPersistenceUnitUtil();
    }

    @After
    public void tearDown() throws Exception
    {
        setup.deleteSchema();
        setup.stopServer();

        em.close();
        emf.close();
    }

    @Test
    public void testIsLoadedObjectString()
    {
        // Persist entity
        Photographer photographer = new Photographer();
        photographer.setPhotographerId(1);
        photographer.setPhotographerName("Amresh");
        Album album = new Album("album1", "My Vacation", "Vacation pics");
        photographer.setAlbum(album);
        em.persist(photographer);
        em.close();

        // Find entity
        em = emf.createEntityManager();
        Photographer p = em.find(Photographer.class, 1);
        Album album2 = p.getAlbum();
        boolean isLoaded = util.isLoaded(album2, "albumName");
        Assert.assertFalse(isLoaded);

        album2.getAlbumName();
        isLoaded = util.isLoaded(p, "album");
        Assert.assertTrue(isLoaded);
    }

    @Test
    public void testIsLoadedObject()
    {
        // Persist entity
        Photographer photographer = new Photographer();
        photographer.setPhotographerId(1);
        photographer.setPhotographerName("Amresh");
        Album album = new Album("album1", "My Vacation", "Vacation pics");
        photographer.setAlbum(album);
        em.persist(photographer);
        em.close();

        // Find entity
        em = emf.createEntityManager();
        Photographer p = em.find(Photographer.class, 1);
        Album album2 = p.getAlbum();
        boolean isLoaded = util.isLoaded(album2);
        Assert.assertFalse(isLoaded);

        album2.getAlbumName();
        isLoaded = util.isLoaded(album2);
        Assert.assertTrue(isLoaded);
    }

    @Test
    public void testGetIdentifier()
    {
        // Persist entity
        Photographer photographer = new Photographer();
        photographer.setPhotographerId(1);
        photographer.setPhotographerName("Amresh");
        Album album = new Album("album1", "My Vacation", "Vacation pics");
        photographer.setAlbum(album);
        em.persist(photographer);
        em.close();

        // Find entity
        em = emf.createEntityManager();
        Photographer p = em.find(Photographer.class, 1);

        Object pk = util.getIdentifier(p);
        Assert.assertEquals(1, pk);

    }
}
