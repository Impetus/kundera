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
package com.impetus.client.hbase.admin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Data handler for HBase queries.
 * 
 * @author impetus
 */

// TODO: Do we really require this interface? If yes, then should we move it
// kundera-core?
public interface DataHandler
{

    /**
     * Creates a HBase table.
     * 
     * @param tableName
     *            table name.
     * @param colFamily
     *            column family.
     */
    void createTableIfDoesNotExist(String tableName, String... colFamily) throws IOException;

    // /**
    // * Writes data help in entity into HBase table
    // *
    // * @param tableName
    // * @param m
    // * @param e
    // * @throws IOException
    // */
    // public void writeData(String tableName, EntityMetadata m, EnhancedEntity
    // e) throws IOException;

    /**
     * Populates data for give column family, column name, and HBase table name.
     */
    Object readData(String tableName, Class clazz, EntityMetadata m, String rowKey, List<String>relationNames) throws IOException;

    void writeData(String tableName, EntityMetadata m, Object entity, String rowId, List<RelationHolder> relations)
            throws IOException;

    /**
     * Writes data into Join Table
     * 
     * @param tableName
     * @param rowId
     * @param columns
     * @throws IOException
     */
    void writeJoinTableData(String tableName, String rowId, Map<String, String> columns) throws IOException;

    /**
     * Retrieves a list of foreign keys from the join table for a given row key
     * 
     * @param <E>
     * @param joinTableName
     * @param rowKey
     * @param inverseJoinColumnName
     * @return
     */
    <E> List<E> getForeignKeysFromJoinTable(String joinTableName, String rowKey, String inverseJoinColumnName);

    /**
     * Shutdown.
     */
    void shutdown();

    /**
     * Delete specific row.
     * @param rowKey
     * @param tableName
     * @throws IOException
     */
    void deleteRow(String rowKey, String tableName) throws IOException;
}