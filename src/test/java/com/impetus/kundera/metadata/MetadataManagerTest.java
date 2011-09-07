/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.metadata;


import junit.framework.TestCase;

import com.impetus.kundera.entity.TwitterUser;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Test case for MetadataManager class
 * @author amresh.singh
 */
public class MetadataManagerTest extends TestCase
{
    MetadataManager metadataManager;
    Class classUnderTest = TwitterUser.class;

    /**
     * @throws java.lang.Exception
     */
    public void setUp() throws Exception
    {
        metadataManager = new MetadataManager();
    }

    /**
     * @throws java.lang.Exception
     */
    public void tearDown() throws Exception
    {
        metadataManager = null;
    }

    /**
     * Test method for {@link com.impetus.kundera.metadata.MetadataManager#buildEntityMetadata(java.lang.Class)}.
     */
    public void testBuildEntityMetadata()
    {
        EntityMetadata metadata = metadataManager.buildEntityMetadata(classUnderTest);
        assertNotNull(metadata);
        
        assertEquals(metadata.getEntityClazz(), TwitterUser.class);
        assertEquals(metadata.getTableName(), "users");
        assertEquals(metadata.getSchema(), "Blog");
        assertEquals(metadata.getPersistenceUnit(), "cassandra");
        
        assertEquals(metadata.getIndexName(), TwitterUser.class.getSimpleName());
        assertEquals(metadata.isIndexable(), true);
        assertEquals(metadata.isCacheable(), false);
        
        assertEquals(metadata.getIdColumn().getName(), "userId");
        assertEquals(metadata.getIdColumn().getField().getName(), "userId");
        assertEquals(metadata.getIdColumn().isIndexable(), false);
        
        assertNotNull(metadata.getReadIdentifierMethod());
        assertNotNull(metadata.getWriteIdentifierMethod());
        
        assertEquals(metadata.getColumnsAsList().size(), 0);
        assertEquals(metadata.getEmbeddedColumnsAsList().size(), 2);
        
        assertEquals(metadata.getRelations().size(), 2);
        
        assertEquals(metadata.getCallbackMethodsMap().entrySet().size(), 0);
        
    }

}
