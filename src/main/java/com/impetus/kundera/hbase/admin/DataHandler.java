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
/**
 * 
 */
package com.impetus.kundera.hbase.admin;

import java.io.IOException;
import java.util.List;

import com.impetus.kundera.hbase.client.HBaseData;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * Data handler for HBase queries.
 * 
 * @author impetus
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
     */
    void createTable(String tableName, String... colFamily) throws IOException;

    /**
     * @param tableName
     * @param columnFamily
     * @param rowKey
     * @param columns
     * @throws IOException
     */
    void loadData(String tableName, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e)
            throws IOException;

    /**
     * Populates data for give column family, column name, and HBase table name.
     * 
     * @param tableName
     *            HBase table name
     * @param columnFamily
     *            column family name
     * @param columnName
     *            column name
     * @param rowKey
     *            HBase row key
     */
    HBaseData populateData(String tableName, String columnFamily, String[] columnName, String rowKey)
            throws IOException;

    /**
     * Shutdown.
     */
    void shutdown();
}
