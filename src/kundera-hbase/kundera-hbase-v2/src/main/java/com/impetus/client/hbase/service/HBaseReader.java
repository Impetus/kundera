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
package com.impetus.client.hbase.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.HBaseDataWrapper;
import com.impetus.client.hbase.Reader;
import com.impetus.client.hbase.utils.HBaseUtils;

/**
 * The Class HBaseReader.
 * 
 * @author Pragalbh Garg
 */
public class HBaseReader implements Reader
{

    /** The scanner. */
    private ResultScanner scanner = null;

    /** The results iter. */
    private Iterator<Result> resultsIter;

    /** The fetch size. */
    private Integer fetchSize;

    /** The counter. */
    private Integer counter = 0;

    /** The table name. */
    private String tableName = null;

    /**
     * Sets the table name.
     * 
     * @param hTable
     *            the new table name
     */
    private void setTableName(Table hTable)
    {
        this.tableName = hTable.getName().getNameAsString();

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Reader#loadData(org.apache.hadoop.hbase.client
     * .Table, java.lang.Object, byte[], byte[], java.lang.String,
     * org.apache.hadoop.hbase.filter.Filter, java.util.List)
     */
    @Override
    public List<HBaseDataWrapper> loadData(Table hTable, Object rowKey, byte[] startRow, byte[] endRow,
            String columnFamily, Filter filter, List<Map<String, Object>> outputColumns) throws IOException
    {
        setTableName(hTable);
        List<HBaseDataWrapper> results = new ArrayList<HBaseDataWrapper>();
        if (rowKey != null)
        {
            startRow = endRow = HBaseUtils.getBytes(rowKey);
        }
        if (scanner == null)
        {
            Scan scan = new Scan();
            if (startRow != null)
            {
                scan.setStartRow(startRow);
            }
            if (endRow != null)
            {
                scan.setStopRow(endRow);
            }
            setScanCriteria(scan, columnFamily, outputColumns, filter);
            scanner = hTable.getScanner(scan);
            resultsIter = scanner.iterator();
        }
        return scanResults(tableName, results);
    }

    /**
     * Sets the scan criteria.
     * 
     * @param scan
     *            the scan
     * @param columnFamily
     *            the column family
     * @param columnsToOutput
     *            the columns to output
     * @param filter
     *            the filter
     */
    private void setScanCriteria(Scan scan, String columnFamily, List<Map<String, Object>> columnsToOutput,
            Filter filter)
    {
        if (filter != null)
        {
            scan.setFilter(filter);
        }
        // if (columnsToOutput != null && !columnsToOutput.isEmpty())
        // {
        // for (Map<String, Object> map : columnsToOutput)
        // {
        // String family = (String) map.get(HBaseUtils.COL_FAMILY);
        // String qualifier = (String) map.get(HBaseUtils.COL_NAME);
        // scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        // }
        // }
    }

    /**
     * Scan results.
     * 
     * @param tableName
     *            the table name
     * @param results
     *            the results
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List<HBaseDataWrapper> scanResults(final String tableName, List<HBaseDataWrapper> results)
            throws IOException
    {
        if (fetchSize == null)
        {
            for (Result result : scanner)
            {
                HBaseDataWrapper data = new HBaseDataWrapper(tableName, result.getRow());
                data.setColumns(result.listCells());
                results.add(data);
            }

            scanner = null;
            resultsIter = null;
        }
        return results;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.client.hbase.Reader#scanRowKeys(org.apache.hadoop.hbase.client
     * .Table, org.apache.hadoop.hbase.filter.Filter, java.lang.String,
     * java.lang.String, java.lang.Class)
     */
    @Override
    public Object[] scanRowKeys(final Table hTable, final Filter filter, final String columnFamilyName,
            final String columnName, final Class rowKeyClazz) throws IOException
    {
        List<Object> rowKeys = new ArrayList<Object>();

        if (scanner == null)
        {
            Scan s = new Scan();
            s.setFilter(filter);
            s.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName));
            scanner = hTable.getScanner(s);
            resultsIter = scanner.iterator();
        }
        if (fetchSize == null)
        {
            for (Result result : scanner)
            {
                for (Cell cell : result.listCells())
                {
                    rowKeys.add(HBaseUtils.fromBytes(CellUtil.cloneFamily(cell), rowKeyClazz));
                }
            }
        }
        if (rowKeys != null && !rowKeys.isEmpty())
        {
            return rowKeys.toArray(new Object[0]);
        }
        return null;
    }

    /**
     * Load all.
     * 
     * @param hTable
     *            the h table
     * @param rows
     *            the rows
     * @param columnFamily
     *            the column family
     * @param columns
     *            the columns
     * @return the list
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public List<HBaseDataWrapper> loadAll(final Table hTable, final List<Object> rows, final String columnFamily,
            final String[] columns) throws IOException
    {
        setTableName(hTable);
        List<HBaseDataWrapper> results = new ArrayList<HBaseDataWrapper>();
        List<Get> getRequest = new ArrayList<Get>();
        for (Object rowKey : rows)
        {
            if (rowKey != null)
            {
                byte[] rowKeyBytes = HBaseUtils.getBytes(rowKey);
                Get request = new Get(rowKeyBytes);
                getRequest.add(request);
            }
        }
        Result[] rawResult = hTable.get(getRequest);
        for (Result result : rawResult)
        {
            List<Cell> cells = result.listCells();
            if (cells != null)
            {
                HBaseDataWrapper data = new HBaseDataWrapper(tableName, result.getRow());
                data.setColumns(cells);
                results.add(data);
            }
        }
        return results;
    }

    /**
     * Sets the fetch size.
     * 
     * @param fetchSize
     *            the new fetch size
     */
    public void setFetchSize(final int fetchSize)
    {
        this.fetchSize = fetchSize;
    }

    /**
     * Next.
     * 
     * @return the h base data
     */
    public HBaseDataWrapper next()
    {
        Result result = resultsIter.next();
        List<Cell> cells = result.listCells();
        HBaseDataWrapper data = new HBaseDataWrapper(tableName, result.getRow());
        data.setColumns(cells);
        return data;
    }

    /**
     * Checks for next.
     * 
     * @return true, if successful
     */
    public boolean hasNext()
    {
        if (scanner == null)
        {
            return false;
        }
        else
        {
            if (fetchSize != null)
            {
                if (counter < fetchSize)
                {
                    counter++;
                    return resultsIter.hasNext();
                }
            }
            else
            {
                return resultsIter.hasNext();
            }
        }
        return false;
    }

    /**
     * Reset.
     */
    public void reset()
    {
        scanner = null;
        fetchSize = null;
        resultsIter = null;
        tableName = null;
        counter = 0;
    }
}
