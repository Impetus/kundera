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

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * @author Pragalbh Garg
 * 
 */
public interface Reader
{

    /**
     * Load data.
     * 
     * @param hTable
     *            the h table
     * @param columnFamily
     *            the column family
     * @param rowKey
     *            the row key
     * @param filter
     *            the filter
     * @param columns
     *            the columns
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List<HBaseData> LoadData(Table hTable, String columnFamily, Object rowKey, Filter filter, String... columns)
            throws IOException;

    /**
     * Load data.
     * 
     * @param hTable
     *            the h table
     * @param rowKey
     *            the row key
     * @param filter
     *            the filter
     * @param columns
     *            the columns
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List<HBaseData> LoadData(Table hTable, Object rowKey, Filter filter, String... columns) throws IOException;

    /**
     * Load all.
     * 
     * @param hTable
     *            the h table
     * @param filter
     *            the filter
     * @param startRow
     *            the start row
     * @param endRow
     *            the end row
     * @param columnFamily
     *            the column family
     * @param qualifier
     *            the qualifier
     * @param columns
     *            the columns
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    List<HBaseData> loadAll(Table hTable, Filter filter, byte[] startRow, byte[] endRow, String columnFamily,
            String qualifier, String[] columns) throws IOException;

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
}
