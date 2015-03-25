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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * The Interface Reader.
 * 
 * @author Pragalbh Garg
 */
public interface Reader
{
    /**
     * Scan row keys.
     * 
     * @param hTable
     *            the h table
     * @param filter
     *            the filter
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
    Object[] scanRowKeys(final Table hTable, final Filter filter, final String columnFamilyName,
            final String columnName, final Class rowKeyClazz) throws IOException;

    /**
     * Load data.
     * 
     * @param hTable
     *            the h table
     * @param rowKey
     *            the row key
     * @param startRow
     *            the start row
     * @param endRow
     *            the end row
     * @param columnFamily
     *            the column family
     * @param filter
     *            the filter
     * @param outputColumns
     *            the output columns
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List<HBaseDataWrapper> loadData(Table hTable, Object rowKey, byte[] startRow, byte[] endRow, String columnFamily,
            Filter filter, List<Map<String, Object>> outputColumns) throws IOException;
}
