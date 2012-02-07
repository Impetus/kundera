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

import org.apache.hadoop.hbase.client.HTable;


/**
 * The Interface Reader.
 */
public interface Reader
{

    /**
     * Populates HBase data for given family name.
     *
     * @param hTable HBase table
     * @param columnFamily HBase column family
     * @param rowKey HBase row key.
     * @return HBase data wrapper containing all column names along with values.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    HBaseData LoadData(HTable hTable, String columnFamily, String rowKey) throws IOException;

    /**
     * Load data.
     *
     * @param hTable the h table
     * @param rowKey the row key
     * @return the h base data
     * @throws IOException Signals that an I/O exception has occurred.
     */
    HBaseData LoadData(HTable hTable, String rowKey) throws IOException;

    /**
     * Load all.
     *
     * @param hTable the h table
     * @param qualifiers the qualifiers
     * @return the h base data
     * @throws IOException Signals that an I/O exception has occurred.
     */
    HBaseData loadAll(HTable hTable, String... qualifiers) throws IOException;

}
