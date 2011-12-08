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
package com.impetus.client.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTable;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.Column;

/**
 * HBase data writer.
 * 
 * @author impetus
 */
public interface Writer
{
	
	void writeColumn(HTable htable, String columnFamily, String rowKey, Column column, Object columnObj) throws IOException;

    /**
     * Writes a column family with name <code>columnFamily</code>, into a table
     * whose columns are <code>columns</code>
     * 
     * @param columnFamily
     *            Column Family Name
     * @param rowKey
     *            Row Key
     * @param columns
     *            Columns for a given column family
     */
    void writeColumns(HTable htable, String columnFamily, String rowKey, List<Column> columns, Object columnFamilyObj)
            throws IOException;

    /**
     * Writes Columns <code>columns</code> into a given table. Each columns is
     * written in their own column family(name same as column name)
     * 
     * @param htable
     * @param rowKey
     * @param columns
     *            Columns of a given table (No column family given)
     * @param entity
     * @throws IOException
     */
    void writeColumns(HTable htable, String rowKey, List<Column> columns, Object entity, List<RelationHolder> relation) throws IOException;


    /**
     * Writes foreign keys along with a database table. They are stored into a
     * column family named FKey-TO. Each column corresponds to foreign key field
     * name and values are actual foreign keys (separated by ~ if applicable)
     * 
     * @param hTable
     * @param rowKey
     * @param foreignKeyMap
     * @throws IOException
     */
    public void writeForeignKeys(HTable hTable, String rowKey, Map<String, Set<String>> foreignKeyMap)
            throws IOException;
}
