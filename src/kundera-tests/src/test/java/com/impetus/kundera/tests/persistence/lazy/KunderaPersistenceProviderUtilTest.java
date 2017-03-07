/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.tests.persistence.lazy;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.spi.LoadState;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.KunderaPersistence;
import com.impetus.kundera.KunderaPersistenceProviderUtil;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * @author amresh.singh Test case for {@link KunderaPersistenceProviderUtil}
 *         Script for running this test case drop keyspace Pickr; create
 *         keyspace Pickr; use Pickr; create column family PHOTOGRAPHER with
 *         comparator=UTF8Type and default_validation_class=UTF8Type and
 *         key_validation_class=UTF8Type and column_metadata=[{column_name:
 *         PHOTOGRAPHER_NAME, validation_class:UTF8Type, index_type:
 *         KEYS},{column_name: ALBUM_ID, validation_class:UTF8Type, index_type:
 *         KEYS}]; create column family ALBUM with comparator=UTF8Type and
 *         default_validation_class=UTF8Type and key_validation_class=UTF8Type
 *         and column_metadata=[{column_name: ALBUM_NAME,
 *         validation_class:UTF8Type, index_type: KEYS},{column_name:
 *         ALBUM_DESC, validation_class:UTF8Type, index_type: KEYS}]; describe
 *         Pickr; list PHOTOGRAPHER; list ALBUM;
 */
public class KunderaPersistenceProviderUtilTest
{
    private static final String KUNDERA_TESTS = "KunderaTests";

    private EntityManagerFactory emf;

    private EntityManager em;

    private KunderaPersistence kp = new KunderaPersistence();

    private KunderaPersistenceProviderUtil util = new KunderaPersistenceProviderUtil(kp);

    private LazyTestSetup setup = new LazyTestSetup();

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

        setup.startServer();
        setup.createSchema();
        
        if(!CassandraCli.keyspaceExist(KUNDERA_TESTS)){
            CassandraCli.createKeySpace(KUNDERA_TESTS);
        }

        Map<String, String> propertyMap = new HashMap<String, String>();
        if (propertyMap.isEmpty())
        {
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "");
            propertyMap.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        }
        Map mapOfExternalProperties = new HashMap<String, Map>();
        mapOfExternalProperties.put("addCassandra", propertyMap);
        mapOfExternalProperties.put("piccandra", propertyMap);
        mapOfExternalProperties.put("secIdxAddCassandra", propertyMap);
        
        emf = Persistence
                .createEntityManagerFactory("rdbms,addMongo,addCassandra,piccandra,secIdxAddCassandra,picongo", mapOfExternalProperties);
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        setup.deleteSchema();
        setup.stopServer();

        if (em != null)
        {
            em.close();
        }
        if (emf != null)
        {
            emf.close();
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.KunderaPersistenceProviderUtil#isLoadedWithReference(java.lang.Object, java.lang.String)}
     * .
     */
    @Test
    public void testIsLoadedWithReference()
    {
        try
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
            // Load state before field referred
            LoadState loadStateWithReference = util.isLoadedWithReference(album2, "albumName");
            // UNKNOWN because PROXY initialization currently not implemented in
            // Kundera
            Assert.assertEquals(LoadState.UNKNOWN, loadStateWithReference);

            // Load state after field referred
            album2.getAlbumName();
            loadStateWithReference = util.isLoadedWithReference(album2, "albumName");
            // UNKNOWN because PROXY initialization currently not implemented in
            // Kundera
            Assert.assertEquals(LoadState.UNKNOWN, loadStateWithReference);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.KunderaPersistenceProviderUtil#isLoadedWithoutReference(java.lang.Object, java.lang.String)}
     * .
     */
    @Test
    public void testIsLoadedWithoutReference()
    {
        try
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

            // Load state before field referred
            // Loaded because LAZY initialization currently not implemented in
            // Kundera
            LoadState loadStateWithoutReference = util.isLoadedWithoutReference(album2, "albumName");
            Assert.assertEquals(LoadState.NOT_LOADED, loadStateWithoutReference);

            // Load state after field referred
            album2.getAlbumName();
            loadStateWithoutReference = util.isLoadedWithoutReference(p, "album");
            Assert.assertEquals(LoadState.LOADED, loadStateWithoutReference);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }

    }

    /**
     * Test method for
     * {@link com.impetus.kundera.KunderaPersistenceProviderUtil#isLoaded(java.lang.Object)}
     * .
     */
    @Test
    public void testIsLoaded()
    {
        try
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

            // Load state before field referred
            LoadState loadState = util.isLoaded(album2);
            Assert.assertEquals(LoadState.NOT_LOADED, loadState);

            // Load state after field referred
            album2.getAlbumName();
            loadState = util.isLoaded(album2);
            Assert.assertEquals(LoadState.LOADED, loadState);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }

}
