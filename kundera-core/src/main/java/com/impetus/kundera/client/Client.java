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

import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;

/**
 * Client API. Defines methods which are required for to be implemented by various clients(pelops, Mongo).
 * Any new addition of new client must implement this API to integrate new client with existing Kundera API. 
 * It's an extension API to support extension of new client.
 * 
 * @author vivek.mishra
 */
public interface Client
{

    /**
     * Retrieve columns from a column-family row.
     * 
     * @param 
     *            the element type
     * @param entityClass
     *            the entity class
     * @param key
     *            The key of the row
     * @return A list of matching columns
     * @throws Exception
     *             the exception
     */
    Object find(Class entityClass, Object key);

    /**
     * Retrieve columns from multiple rows of a column-family.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param keys
     *            Array of row keys
     * @return A Map of row and corresponding list of columns.
     * @throws Exception
     *             the exception
     */
    //I will come to it LATER. 
    <E> List<E> findAll(Class<E> entityClass, Object... keys);

    /**
     * Load data.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param embeddedColumnMap
     *            the col
     * @return the list
     * @throws Exception
     *             the exception
     */
    <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap);

    /**
     * Shutdown.
     */
    void close();

    /**
     * Delete.
     * 
     * @param entity
     *            the entity
     * @param pKey
     *            the key
     * @throws Exception
     *             the exception
     */
    void delete(Object entity, Object pKey);

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
     * On persistence.
     * 
     * @param entitySaveGraph
     *            entity save graph
     * @param metadata
     *            entity meta data
     * @return id id of persisted entity.
     */
    @Deprecated
    String persist(EntitySaveGraph entitySaveGraph, EntityMetadata metadata);

    /**
     * On persistence.
     * 
     * @param childEntity
     *            child entity
     * @param entitySaveGraph
     *            entity save graph
     * @param metadata
     *            entity meta data
     * @return id id of persisted entity.
     */
    @Deprecated
    void persist(Object childEntity, EntitySaveGraph entitySaveGraph, EntityMetadata metadata);
    
    void persist(Node node);

    /**
     * Inserts records into Join Table.
     * 
     * @param joinTableName
     *            Name of Join Table
     * @param joinColumnName
     *            Name of Join Column
     * @param inverseJoinColumnName
     *            Name of Inverse Join Column
     * @param relMetadata
     *            Entity metadata for the child entity (i.e. entity at the other
     *            side of the relationship)
     * @param primaryKey
     *            TODO
     * @param childEntity
     *            TODO
     */
    void persistJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            EntityMetadata relMetadata, Object primaryKey, Object childEntity);

    /**
     * Retrieves a list of foreign keys from a join table for a given primary
     * key.
     * 
     * @param <E>
     *            the element type
     * @param joinTableName
     *            Name of Join Table
     * @param joinColumnName
     *            Name of Join Column
     * @param inverseJoinColumnName
     *            Name of Inverse Join Column
     * @param relMetadata
     *            Entity metadata for the child entity (i.e. entity at the other
     *            side of the relationship)
     * @param objectGraph
     *            Object graph of the persistence (Includes parent and child
     *            data and other related info)
     * @return the foreign keys from join table
     */
    <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String joinColumnName, String inverseJoinColumnName,
            EntityMetadata relMetadata, EntitySaveGraph objectGraph);

    <E> List<E> findParentEntityFromJoinTable(EntityMetadata parentMetadata, String joinTableName,
            String joinColumnName, String inverseJoinColumnName, Object childId);

    /**
     * Delete rows from given table for given column name and corresponding value..
     * @param tableName
     *               Name of the table
     * @param columnName
     *               Name of the column
     * @param columnValue
     *               Name of column value                         
     */
    void deleteByColumn(String tableName, String columnName, Object columnValue);
    
    /**
     * Find list of entities for given column name and column value, if index
     * support is provided..
     * 
     * @param colName
     *            the column name
     * @param colValue
     *            the column value
     * @param entityClass
     *            the entity class           
     * @return the list list of entities.
     */
    List<Object> findByRelation(String colName, String colValue, Class entityClazz);

    /**
     * Returns entity reader instance bind to specific client.
     * 
     * @return reader entity reader.
     */
    EntityReader getReader();
}
