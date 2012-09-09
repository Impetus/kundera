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
package com.impetus.kundera;

import java.lang.reflect.Method;

import javax.persistence.spi.LoadState;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.configure.MetamodelConfiguration;
import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.entity.album.AlbumUni_1_1_1_1;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_1_1_1;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.proxy.KunderaProxy;

/**
 * @author amresh.singh
 *
 */
public class KunderaPersistenceProviderUtilTest
{
    KunderaPersistence kp = new KunderaPersistence();
    KunderaPersistenceProviderUtil util = new KunderaPersistenceProviderUtil(kp);
    
    

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        //new PersistenceUnitConfiguration("kunderatest").configure();
        //new MetamodelConfiguration("kunderatest").configure();
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
        /*try
        {
            PhotographerUni_1_1_1_1 photographer = new PhotographerUni_1_1_1_1();
            EntityMetadata m = KunderaMetadataManager.getEntityMetadata(photographer.getClass());
            
            String entityName = PhotographerUni_1_1_1_1.class.getName() + "_" + "1" + "#"
            + "album";
            
            Object albumProxy = getLazyEntity(entityName, AlbumUni_1_1_1_1.class, m.getReadIdentifierMethod(), m.getWriteIdentifierMethod(), "album1", null);
            photographer.setAlbum((AlbumUni_1_1_1_1)albumProxy);
            
            LoadState loadStateWithReference = util.isLoadedWithReference(photographer, "album");
            LoadState loadStateWithoutReference = util.isLoadedWithoutReference(photographer, "album");           
            System.out.println(loadStateWithReference + ", " + loadStateWithoutReference);
            
            AlbumUni_1_1_1_1 album = new AlbumUni_1_1_1_1();
            photographer.setAlbum(album);
            loadStateWithReference = util.isLoadedWithReference(photographer, "album");
            loadStateWithoutReference = util.isLoadedWithoutReference(photographer, "album");           
            System.out.println(loadStateWithReference + ", " + loadStateWithoutReference);
            
            AlbumUni_1_1_1_1 album2 = photographer.getAlbum();
            loadStateWithReference = util.isLoadedWithReference(photographer, "album");
            loadStateWithoutReference = util.isLoadedWithoutReference(photographer, "album");           
            System.out.println(loadStateWithReference + ", " + loadStateWithoutReference);
            
            
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }*/
    }

    /**
     * Test method for {@link com.impetus.kundera.KunderaPersistenceProviderUtil#isLoaded(java.lang.Object)}.
     */
    @Test
    public void testIsLoaded()
    {
        
    }
    
    private KunderaProxy getLazyEntity(String entityName, Class<?> persistentClass, Method getIdentifierMethod,
            Method setIdentifierMethod, String id, PersistenceDelegator pd)
    {
        return KunderaMetadata.INSTANCE.getCoreMetadata().getLazyInitializerFactory().getProxy(entityName, persistentClass, getIdentifierMethod, setIdentifierMethod, id, pd);
    }

}
