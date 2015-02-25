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
package com.impetus.kundera.index.lucene;

import java.util.Map;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * Interface to define the behavior of an Indexer.
 * 
 * @author animesh.kumar
 */
public interface Indexer extends com.impetus.kundera.index.Indexer
{

    /**
     * Unindexed an entity with key:id.
     * 
     * @param metadata
     *            the metadata
     * @param id
     *            the id
     */

    void unindex(EntityMetadata metadata, Object id,KunderaMetadata kunderaMetadata, Class<?> parentClazz);

    /**
     * Indexes and object.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     */
    void index(EntityMetadata metadata, final MetamodelImpl metaModel, Object object);

    /**
     * Indexes and object.
     * 
     * @param metadata
     *            the meta data.
     * @param object
     *            the object.
     * @param parentId
     *            parent Id.
     * @param clazz
     *            parent class.
     */
    void index(EntityMetadata metadata, final MetamodelImpl metaModel, Object object, String parentId, Class<?> clazz);

    /**
     * Searches for an object. Note that the "query" must be in Indexer
     * specified form.
     * 
     * @param luceneQuery
     *            the lucene query
     * @param start
     *            the start
     * @param count
     *            the count
     * @param fetchRelation
     *            the fetch relation
     * @return the list
     */

    Map<String, Object> search(String luceneQuery, int start, int count, boolean fetchRelation,
            KunderaMetadata kunderaMetadata, EntityMetadata metadata);

    

    /**
     * Close on index writer/reader.
     */
    void close();

    /**
     * Flushes out indexes.
     */
    void flush();

    /**
     * Validates, if document exists in index.
     * 
     * @param metadata entity metadata
     * @param id   entity id
     * @return true, if exists else false.
     */
    

    
    /**
     * Updates the existing document.
     * 
     * @param metadata   entity metadata.
     * @param entity     entity object. 
     * @param id         entity id
     * @param parentId   parent entity id
     * @param parentClazz  parent class
     */
    void update(EntityMetadata metadata, final MetamodelImpl metaModel, Object entity, Object id, String parentId);

    /**
     * @param entityClass
     * @param kunderaMetadata
     * @param metadata
     * @return
     */
    boolean entityExistsInIndex(Class<?> entityClass, KunderaMetadata kunderaMetadata, EntityMetadata metadata);

    /**
     * @param metadata
     * @param id
     * @param kunderaMetadata
     * @param isEmbeddedId
     * @param parentClazz
     * @return
     */
    boolean documentExistsInIndex(EntityMetadata metadata, Object id, KunderaMetadata kunderaMetadata,
            boolean isEmbeddedId, Class<?> parentClazz);

    
}