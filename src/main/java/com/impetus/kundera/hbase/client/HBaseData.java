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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;

import com.impetus.kundera.DataWrapper;

/**
 * @author impetus
 */
public class HBaseData implements DataWrapper
{

    private String columnFamily;

    private String rowKey;

    private List<KeyValue> columns;

    /**
     * constructor with fields.
     * 
     * @param columnFamily
     *            HBase column family
     * @param rowKey
     *            Row key
     */
    public HBaseData(String columnFamily, String rowKey)
    {
        this.columnFamily = columnFamily;
        this.rowKey = rowKey;
    }

    /**
     * Getter column family
     * 
     * @return columnFamily column family
     */
    public String getColumnFamily()
    {
        return columnFamily;
    }

    /**
     * Getter for row key
     * 
     * @return rowKey
     */
    public String getRowKey()
    {
        return rowKey;
    }

    /**
     * Getter for list of columns.
     * 
     * @return list of columns
     */
    public List<KeyValue> getColumns()
    {
        return Collections.unmodifiableList(columns);
    }

    /**
     * @param columns
     */
    public void setColumns(List<KeyValue> columns)
    {
        this.columns = columns;
    }

}
