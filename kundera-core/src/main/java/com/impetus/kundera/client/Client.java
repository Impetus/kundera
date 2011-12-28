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
package com.impetus.kundera.client;

import java.util.List;
import java.util.Map;

import javax.persistence.Query;

import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * The Interface Client.
 * 
 * @author impetus
 */
public interface Client
{

    /**
     * Writes Multiple columns.
     *
     * @param e Ehanced Entity
     * @throws Exception the exception
     */
    @Deprecated
    void persist(EnhancedEntity e) throws Exception;

    /**
     * Retrieve columns from a column-family row.
     *
     * @param <E> the element type
     * @param entityClass the entity class
     * @param key The key of the row
     * @return A list of matching columns
     * @throws Exception the exception
     */
    <E> E find(Class<E> entityClass, String key) throws Exception;

    /**
     * Retrieve columns from multiple rows of a column-family.
     *
     * @param <E> the element type
     * @param entityClass the entity class
     * @param keys Array of row keys
     * @return A Map of row and corresponding list of columns.
     * @throws Exception the exception
     */
    <E> List<E> find(Class<E> entityClass, String... keys) throws Exception;

    /**
     * Load data.
     *
     * @param <E> the element type
     * @param entityClass the entity class
     * @param embeddedColumnMap the col
     * @return the list
     * @throws Exception the exception
     */
    <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) throws Exception;

    /**
     * Loads columns from multiple rows restricting results to conditions stored
     * in <code>filterClauseQueue</code>.
     *
     * @param <E> the element type
     * @param query the query
     * @return the list
     * @throws Exception the exception
     */
    @SuppressWarnings("unchecked")
    <E> List<E> loadData(Query query) throws Exception;

    /**
     * Shutdown.
     */
    void close();


    /**
     * 
     * @param entity
     * @param pKey
     * @param metadata
     * @throws Exception
     */
    void delete(Object entity, Object pKey, EntityMetadata metadata) throws Exception;

    /**
     * Gets the persistence unit.
     *
     * @return the persistence unit
     */
    String getPersistenceUnit();

    // TODO Do we really need it. This may not be required for few of the
    // clients
    /**
     * Gets the index manager.
     *
     * @return the index manager
     */
    IndexManager getIndexManager();

    /**
     * Sets the persistence unit.
     *
     * @param persistenceUnit the new persistence unit
     */
    void setPersistenceUnit(String persistenceUnit);

    /**
     *  On persistence
     * @param entitySaveGraph              entity save graph
     * @param metadata                     entity meta data
     * @return id                          id of persisted entity.
     */
    String persist(EntitySaveGraph entitySaveGraph, EntityMetadata metadata);
    
    /**
     *  On persistence
     *  @param childEntity                 child entity
     * @param entitySaveGraph              entity save graph
     * @param metadata                     entity meta data
     * @return id                          id of persisted entity.
     */
    void  persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata); 
    
    void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName, EntityMetadata relMetadata, EntitySaveGraph objectGraph);
    
    
    /**
     * 
     * @param clazz
     * @param metadata
     * @param rowId
     * @return
     */
    Object find(Class<?> clazz, EntityMetadata metadata, String rowId);
}
