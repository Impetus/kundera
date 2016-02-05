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
package com.impetus.client.hbase.admin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Data handler for HBase queries.
 * 
 * @author vivek.mishra
 */

public interface DataHandler
{

    /**
     * Creates a HBase table.
     * 
     * @param tableName
     *            table name.
     * @param colFamily
     *            column family.
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void createTableIfDoesNotExist(TableName tableName, String... colFamily) throws IOException;

    /**
     * Populates data for give column family, column name, and HBase table name.
     * 
     * @param table
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param rowKey
     *            the row key
     * @param f
     * @param relationNames
     *            the relation names
     * @return the object
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List readData(Table table, Class clazz, EntityMetadata m, Object rowKey, List<String> relationNames,
            FilterList f, String... columns) throws IOException;

    /**
     * Populates data for give column family, column name, and HBase table name.
     * 
     * @param table
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param rowKeys
     *            the row key
     * @param relationNames
     *            the relation names
     * @return the object
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List readAll(Table table, Class clazz, EntityMetadata m, List<Object> rowKeys, List<String> relationNames,
            String... columns) throws IOException;

    /**
     * @param clazz
     * @param m
     * @param startRow
     * @param endRow
     * @param columns
     * @param f
     * @return
     */
    List readDataByRange(Table table, Class clazz, EntityMetadata m, byte[] startRow, byte[] endRow,
            String[] columns, FilterList f) throws IOException;

    /**
     * Write data.
     * 
     * @param table
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param rowId
     *            the row id
     * @param relations
     *            the relations
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void writeData(Table table, EntityMetadata m, Object entity, Object rowId, List<RelationHolder> relations,
            boolean showQuery) throws IOException;

    /**
     * Writes data into Join Table.
     * 
     * @param table
     * @param rowId
     *            the row id
     * @param columns
     *            the columns
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void writeJoinTableData(Table table, Object rowId, Map<String, Object> columns, String columnFamilyName)
            throws IOException;

    /**
     * Retrieves a list of foreign keys from the join table for a given row key.
     * 
     * @param <E>
     *            the element type
     * @param joinTableName
     *            the join table name
     * @param rowKey
     *            the row key
     * @param inverseJoinColumnName
     *            the inverse join column name
     * @return the foreign keys from join table
     */
    <E> List<E> getForeignKeysFromJoinTable(String schemaName, String joinTableName, Object rowKey,
            String inverseJoinColumnName);

    /**
     * Retrieves a list of parent entity from join table..
     * 
     * @param <E>
     * @param parentMetadata
     * @param joinTableName
     * @param joinColumnName
     * @param inverseJoinColumnName
     * @param childId
     * @return
     */
    <E> List<E> findParentEntityFromJoinTable(EntityMetadata parentMetadata, String joinTableName,
            String joinColumnName, String inverseJoinColumnName, Object childId);

    /**
     * Shutdown.
     */
    void shutdown();

    /**
     * Delete specific row.
     * 
     * @param rowKey
     *            the row key
     * @param table
     *            the table name
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void deleteRow(Object rowKey, Table table, String columnFamilyName) throws IOException;

    Object[] scanRowyKeys(FilterList filterList, Table table, String columnFamilyName, String columnName,
            Class rowKeyClazz) throws IOException;
}
