/*
 * Copyright 2011 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;

public interface Reader
{

    /**
     * Populates HBase data for given family name.
     * 
     * @param hTable
     *            HBase table
     * @param columnFamily
     *            HBase column family
     * @param columnName
     *            HBase column name.
     * @param rowKey
     *            HBase row key.
     * @return HBase data wrapper containing all column names along with values.
     */
    HBaseData LoadData(HTable hTable, String columnFamily, String[] columnName, String rowKey) throws IOException;

    /**
     * 
     * @param hTable
     * @param qualifiers
     * @return
     */
    HBaseData loadAll(HTable hTable, String... qualifiers) throws IOException;

}
