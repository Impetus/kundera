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
package com.impetus.client.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Table;

import com.impetus.client.hbase.admin.HBaseRow;

/**
 * The Interface Writer.
 * 
 * @author Pragalbh Garg
 */
public interface Writer
{
    /**
     * Writes columns data to HBase table, supplied as a map in Key/ value pair;
     * key and value representing column name and value respectively.
     * 
     * @param htable
     *            the htable
     * @param rowKey
     *            the row key
     * @param columns
     *            the columns
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void writeColumns(Table htable, Object rowKey, Map<String, Object> columns, String columnFamilyName)
            throws IOException;

    /**
     * Delete.
     * 
     * @param hTable
     *            the h table
     * @param rowKey
     *            the row key
     * @param colFamily
     *            the col family
     * @param colName
     *            the col name
     */
    void delete(Table hTable, Object rowKey, String colFamily, String colName);

    /**
     * Write row.
     * 
     * @param hTable
     *            the h table
     * @param hbaseRow
     *            the hbase row
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    void writeRow(Table hTable, HBaseRow hbaseRow) throws IOException;
}
