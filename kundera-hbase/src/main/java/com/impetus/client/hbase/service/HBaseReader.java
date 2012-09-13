/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.hbase.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.HBaseData;
import com.impetus.client.hbase.Reader;
import com.impetus.client.hbase.utils.HBaseUtils;

/**
 * Implmentation class for HBase for <code>Reader</code> interface.
 * 
 * @author vivek.mishra
 */
public class HBaseReader implements Reader
{

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Reader#LoadData(org.apache.hadoop.hbase.client
     * .HTable, java.lang.String, java.lang.String)
     */
    @SuppressWarnings("unused")
    @Override
    public List<HBaseData> LoadData(HTable hTable, String columnFamily, Object rowKey, Filter filter)
            throws IOException
    {
        List<HBaseData> results = null;

        // Get g = prepareGet(rowKey, filter);

        ResultScanner scanner = null;

        // only in case of find by id
        Scan scan = null;
        if (rowKey != null)
        {
            byte[] rowKeyBytes = HBaseUtils.getBytes(rowKey);
            Get g = new Get(rowKeyBytes);
            scan = new Scan(g);
        }
        else
        {
            scan = new Scan();
        }
        setScanCriteria(filter, columnFamily, scan);
        scanner = hTable.getScanner(scan);

        return scanResults(columnFamily, results, scanner);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Reader#LoadData(org.apache.hadoop.hbase.client
     * .HTable, java.lang.String)
     */
    @Override
    public List<HBaseData> LoadData(HTable hTable, Object rowKey, Filter filter) throws IOException
    {
        return LoadData(hTable, null, rowKey, filter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Reader#loadAll(org.apache.hadoop.hbase.client
     * .HTable, org.apache.hadoop.hbase.filter.Filter, byte[], byte[])
     */
    @Override
    public List<HBaseData> loadAll(HTable hTable, Filter filter, byte[] startRow, byte[] endRow, String columnFamily)
            throws IOException
    {
        List<HBaseData> results = null;
        Scan s = null;
        if (startRow != null && endRow != null)
        {
            s = new Scan(startRow, endRow);
        }
        else if (startRow != null)
        {
            s = new Scan(startRow);
        }
        else
        {
            s = new Scan();
        }

        setScanCriteria(filter, columnFamily, s);

        ResultScanner scanner = hTable.getScanner(s);
        return scanResults(null, results, scanner);
    }

    /**
     * @param filter
     * @param columnFamily
     * @param s
     */
    private void setScanCriteria(Filter filter, String columnFamily, Scan s)
    {
        if (filter != null)
        {
            s.setFilter(filter);
        }
        if (columnFamily != null)
        {
            s.addFamily(Bytes.toBytes(columnFamily));
        }
    }

    /**
     * Scan and populate {@link HBaseData} collection using scanned results.
     * 
     * @param columnFamily
     *            column family.
     * @param results
     *            results.
     * @param scanner
     *            result scanner.
     * @return collection of scanned results.
     */
    private List<HBaseData> scanResults(String columnFamily, List<HBaseData> results, ResultScanner scanner)
    {
        HBaseData data = null;
        for (Result result : scanner)
        {
            List<KeyValue> values = result.list();
            for (KeyValue value : values)
            {
                data = new HBaseData(columnFamily != null ? columnFamily : new String(value.getFamily()),
                        value.getRow());
                break;
            }
            data.setColumns(values);
            if (results == null)
            {
                results = new ArrayList<HBaseData>();
            }
            results.add(data);
        }

        return results;
    }

    @Override
    public Object[] scanRowKeys(final HTable hTable, final Filter filter, final String columnFamilyName,
            final String columnName) throws IOException
    {
        List<Object> rowKeys = new ArrayList<Object>();
        Scan s = new Scan();
        s.setFilter(filter);
        s.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));

        ResultScanner scanner = hTable.getScanner(s);

        for (Result result : scanner)
        {
            for (KeyValue keyValue : result.list())
            {
                rowKeys.add(keyValue.getKey());
            }
        }

        if (rowKeys != null && !rowKeys.isEmpty())
        {
            return rowKeys.toArray(new Object[0]);
        }
        return null;
    }
}
