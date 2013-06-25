/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.persistence.context;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for {@link PersistenceCacheManager}
 * @author amresh.singh
 *
 */
public class PersistenceCacheManagerTest
{
    PersistenceCacheManager pcm;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        PersistenceCache pc = new PersistenceCache();
        pcm = new PersistenceCacheManager(pc);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        pcm = null;
        
    }


    /**
     * Test method for {@link com.impetus.kundera.persistence.context.PersistenceCacheManager#clearPersistenceCache()}.
     */
    @Test
    public void testClearPersistenceCache()
    {
    }

    /**
     * Test method for {@link com.impetus.kundera.persistence.context.PersistenceCacheManager#markAllNodesNotTraversed()}.
     */
    @Test
    public void testMarkAllNodesNotTraversed()
    {

    }

    /**
     * Test method for {@link com.impetus.kundera.persistence.context.PersistenceCacheManager#addEntityToPersistenceCache(java.lang.Object, com.impetus.kundera.persistence.PersistenceDelegator, java.lang.Object)}.
     */
    @Test
    public void testAddEntityToPersistenceCache()
    {

    }
    
    private void buildNodes()
    {
       // pcm.addEntityToPersistenceCache(new Perso, pd, entityId)
    }

}
