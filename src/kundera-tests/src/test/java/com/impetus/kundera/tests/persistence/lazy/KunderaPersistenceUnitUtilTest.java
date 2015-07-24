/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.tests.persistence.lazy;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceUnitUtil;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;

/**
 * 
 * @author Amresh.singh junit for {@link PersistenceUnitUtil}
 * 
 */
public class KunderaPersistenceUnitUtilTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    private PersistenceUnitUtil util;

    private LazyTestSetup setup = new LazyTestSetup();

    @Before
    public void setUp() throws Exception
    {
        setup.startServer();
        setup.createSchema();

        Map<String, String> propertyMap = new HashMap<String, String>();
        if (propertyMap.isEmpty())
        {
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "update");
            
        }
        Map mapOfExternalProperties = new HashMap<String, Map>();
        mapOfExternalProperties.put("addCassandra", propertyMap);
        mapOfExternalProperties.put("piccandra", propertyMap);
        mapOfExternalProperties.put("secIdxAddCassandra", propertyMap);
        emf = Persistence.createEntityManagerFactory(
                "addCassandra,rdbms,redis,addMongo,piccandra,secIdxAddCassandra,picongo", mapOfExternalProperties);
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
