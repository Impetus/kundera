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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.spi.LoadState;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.KunderaPersistence;
import com.impetus.kundera.KunderaPersistenceProviderUtil;

/**
 * @author amresh.singh
 * Script for running this test case
 *         drop keyspace Pickr;
 *         create keyspace Pickr;
 *         use Pickr;
 *         create column family PHOTOGRAPHER with comparator=UTF8Type and
 *         default_validation_class=UTF8Type and key_validation_class=UTF8Type
 *         and column_metadata=[{column_name: PHOTOGRAPHER_NAME,
 *         validation_class:UTF8Type, index_type: KEYS},{column_name: ALBUM_ID,
 *         validation_class:UTF8Type, index_type: KEYS}]; 
 *         create column family
 *         ALBUM with comparator=UTF8Type and default_validation_class=UTF8Type
 *         and key_validation_class=UTF8Type and column_metadata=[{column_name:
 *         ALBUM_NAME, validation_class:UTF8Type, index_type:
 *         KEYS},{column_name: ALBUM_DESC, validation_class:UTF8Type,
 *         index_type: KEYS}]; 
 *         describe Pickr; list PHOTOGRAPHER; list ALBUM;
 */
public class KunderaPersistenceProviderUtilTest
{
    EntityManagerFactory emf;
    EntityManager em;
    
   KunderaPersistence kp = new KunderaPersistence();
   KunderaPersistenceProviderUtil util = new KunderaPersistenceProviderUtil(kp);
    
    

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("piccandra");
        em = emf.createEntityManager();        
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Test method for {@link com.impetus.kundera.KunderaPersistenceProviderUtil#isLoadedWithoutReference(java.lang.Object, java.lang.String)}.
     */
    @Test
    public void testIsLoadedWithoutReference()
    {
        
    }

    /**
     * Test method for {@link com.impetus.kundera.KunderaPersistenceProviderUtil#isLoadedWithReference(java.lang.Object, java.lang.String)}.
     */
    @Test
    public void testIsLoadedWithReference()
    {
        try
        {
            Photographer photographer = new Photographer();
            photographer.setPhotographerId(1);
            photographer.setPhotographerName("Amresh");
            Album album = new Album("album1", "My Vacation", "Vacation pics");
            photographer.setAlbum(album);           
            
            em.persist(photographer);   
            em.close();
            
            em = emf.createEntityManager();
            
            Photographer p = em.find(Photographer.class, 1);
            Album album2 = p.getAlbum();
            LoadState loadStateWithReference = util.isLoadedWithReference(album2, "albumName");
            LoadState loadStateWithoutReference = util.isLoadedWithoutReference(album2, "albumName");           
            System.out.println(loadStateWithReference + ", " + loadStateWithoutReference);    
            
            
            album2.getAlbumName();
            loadStateWithReference = util.isLoadedWithReference(album2, "albumName");
            loadStateWithoutReference = util.isLoadedWithoutReference(album2, "albumName");           
            System.out.println(loadStateWithReference + ", " + loadStateWithoutReference);
            
            
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Test method for {@link com.impetus.kundera.KunderaPersistenceProviderUtil#isLoaded(java.lang.Object)}.
     */
    @Test
    public void testIsLoaded()
    {
        
    } 


}
