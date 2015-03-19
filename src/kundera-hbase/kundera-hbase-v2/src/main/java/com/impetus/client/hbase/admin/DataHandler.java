/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.filter.FilterList;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * The Interface DataHandler.
 * 
 * @author Pragalbh Garg
 */
public interface DataHandler
{

    /**
     * Creates the table if does not exist.
     * 
     * @param tableName
     *            the table name
     * @param colFamily
     *            the col family
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void createTableIfDoesNotExist(String tableName, String... colFamily) throws IOException;

    /**
     * Read data.
     * 
     * @param tableName
     *            the table name
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param rowKey
     *            the row key
     * @param relatationNames
     *            the relatation names
     * @param f
     *            the f
     * @param colToOutput
     *            the col to output
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List readData(String tableName, Class clazz, EntityMetadata m, Object rowKey, List<String> relatationNames,
            FilterList f, List<Map<String, Object>> colToOutput) throws IOException;

    /**
     * Read all.
     * 
     * @param tableName
     *            the table name
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param rowKeys
     *            the row keys
     * @param relatationNames
     *            the relatation names
     * @param columns
     *            the columns
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List readAll(String tableName, Class clazz, EntityMetadata m, List<Object> rowKeys, List<String> relatationNames,
            String... columns) throws IOException;

    /**
     * Read data by range.
     * 
     * @param tableName
     *            the table name
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param startRow
     *            the start row
     * @param endRow
     *            the end row
     * @param colToOutput
     *            the col to output
     * @param f
     *            the f
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List readDataByRange(String tableName, Class clazz, EntityMetadata m, byte[] startRow, byte[] endRow,
            List<Map<String, Object>> colToOutput, FilterList f) throws IOException;

    /**
     * Write data.
     * 
     * @param schemaName
     *            the schema name
     * @param m
     *            the m
     * @param entity
     *            the entity
     * @param rowId
     *            the row id
     * @param relations
     *            the relations
     * @param showQuery
     *            the show query
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void writeData(String schemaName, EntityMetadata m, Object entity, Object rowId, List<RelationHolder> relations,
            boolean showQuery) throws IOException;

    /**
     * Write join table data.
     * 
     * @param tableName
     *            the table name
     * @param rowId
     *            the row id
     * @param columns
     *            the columns
     * @param columnFamilyName
     *            the column family name
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void writeJoinTableData(String tableName, Object rowId, Map<String, Object> columns, String columnFamilyName)
            throws IOException;

    /**
     * Gets the foreign keys from join table.
     * 
     * @param <E>
     *            the element type
     * @param schemaName
     *            the schema name
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
     * Find parent entity from join table.
     * 
     * @param <E>
     *            the element type
     * @param parentMetadata
     *            the parent metadata
     * @param joinTableName
     *            the join table name
     * @param joinColumnName
     *            the join column name
     * @param inverseJoinColumnName
     *            the inverse join column name
     * @param childId
     *            the child id
     * @return the list
     */
    <E> List<E> findParentEntityFromJoinTable(EntityMetadata parentMetadata, String joinTableName,
            String joinColumnName, String inverseJoinColumnName, Object childId);

    /**
     * Shutdown.
     */
    void shutdown();

    /**
     * Delete row.
     * 
     * @param rowKey
     *            the row key
     * @param colName
     *            the col name
     * @param colFamily
     *            the col family
     * @param tableName
     *            the table name
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void deleteRow(Object rowKey, String colName, String colFamily, String tableName) throws IOException;

    /**
     * Scan rowy keys.
     * 
     * @param filterList
     *            the filter list
     * @param tableName
     *            the table name
     * @param columnFamilyName
     *            the column family name
     * @param columnName
     *            the column name
     * @param rowKeyClazz
     *            the row key clazz
     * @return the object[]
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    Object[] scanRowyKeys(FilterList filterList, String tableName, String columnFamilyName, String columnName,
            Class rowKeyClazz) throws IOException;

    /**
     * Prepare put.
     * 
     * @param hbaseRow
     *            the hbase row
     * @return the put
     */
    Put preparePut(HBaseRow hbaseRow);

    /**
     * Prepare delete.
     * 
     * @param rowKey
     *            the row key
     * @return the row
     */
    Row prepareDelete(Object rowKey);

    /**
     * Batch process.
     * 
     * @param batchData
     *            the batch data
     */
    void batchProcess(Map<String, List<Row>> batchData);
}
