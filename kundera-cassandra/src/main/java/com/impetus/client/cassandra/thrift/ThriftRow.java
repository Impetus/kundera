/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.cassandra.thrift;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.SuperColumn;

/**
 * Utility class that represents a row in Cassandra DB.
 * 
 * @author amresh.singh
 */
public class ThriftRow
{
    /** Id of the row. */
    private Object id;

    /** name of the family. */
    private String columnFamilyName;

    /** list of thrift columns from the row. */
    private List<Column> columns;

    /** list of thrift super columns columns from the row. */
    private List<SuperColumn> superColumns;

    /** list of thrift counter columns from the row. */
    private List<CounterColumn> counterColumns;

    /** list of thrift counter super columns columns from the row. */
    private List<CounterSuperColumn> counterSuperColumns;

    /**
     * default constructor.
     */
    public ThriftRow()
    {
        columns = new ArrayList<Column>();
        superColumns = new ArrayList<SuperColumn>();
        counterColumns = new ArrayList<CounterColumn>();
        counterSuperColumns = new ArrayList<CounterSuperColumn>();
    }

    /**
     * The Constructor.
     * 
     * @param id
     *            the id
     * @param columnFamilyName
     *            the column family name
     * @param columns
     *            the columns
     * @param superColumns
     *            the super columns
     */
    public ThriftRow(Object id, String columnFamilyName, List<Column> columns, List<SuperColumn> superColumns,
            List<CounterColumn> counterColumns, List<CounterSuperColumn> counterSuperColumns)
    {
        this.id = id;
        this.columnFamilyName = columnFamilyName;
        if (columns != null)
        {
            this.columns = columns;
        }

        if (superColumns != null)
        {
            this.superColumns = superColumns;
        }
        if (counterColumns != null)
        {
            this.counterColumns = counterColumns;
        }

        if (counterSuperColumns != null)
        {
            this.counterSuperColumns = counterSuperColumns;
        }
    }

    /**
     * Adds the column.
     * 
     * @param column
     *            the column
     */
    public void addColumn(Column column)
    {
        columns.add(column);
    }

    /**
     * @return the id
     */
    public Object getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(Object id)
    {
        this.id = id;
    }

    /**
     * @return the columnFamilyName
     */
    public String getColumnFamilyName()
    {
        return columnFamilyName;
    }

    /**
     * @param columnFamilyName
     *            the columnFamilyName to set
     */
    public void setColumnFamilyName(String columnFamilyName)
    {
        this.columnFamilyName = columnFamilyName;
    }

    /**
     * @return the columns
     */
    public List<Column> getColumns()
    {
        return columns;
    }

    /**
     * @param columns
     *            the columns to set
     */
    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    /**
     * @return the superColumns
     */
    public List<SuperColumn> getSuperColumns()
    {
        return superColumns;
    }

    /**
     * @param superColumns
     *            the superColumns to set
     */
    public void setSuperColumns(List<SuperColumn> superColumns)
    {
        this.superColumns = superColumns;
    }

    /**
     * Adds the super column.
     * 
     * @param superColumn
     *            the super column
     */
    public void addSuperColumn(SuperColumn superColumn)
    {
        this.superColumns.add(superColumn);
    }

    /**
     * @return the counterColumns
     */
    public List<CounterColumn> getCounterColumns()
    {
        return counterColumns;
    }

    /**
     * @param counterColumns
     *            the counterColumns to set
     */
    public void setCounterColumns(List<CounterColumn> counterColumns)
    {
        this.counterColumns = counterColumns;
    }

    /**
     * Adds the counter column.
     * 
     * @param counter
     *            column the column
     */
    public void addCounterColumn(CounterColumn column)
    {
        counterColumns.add(column);
    }

    /**
     * @return the counterSuperColumns
     */
    public List<CounterSuperColumn> getCounterSuperColumns()
    {
        return counterSuperColumns;
    }

    /**
     * @param counterSuperColumns
     *            the counterSuperColumns to set
     */
    public void setCounterSuperColumns(List<CounterSuperColumn> counterSuperColumns)
    {
        this.counterSuperColumns = counterSuperColumns;
    }

    /**
     * Adds the counter super column.
     * 
     * @param countersuperColumn
     *            the super column
     */
    public void addCounterSuperColumn(CounterSuperColumn superColumn)
    {
        this.counterSuperColumns.add(superColumn);
    }

}
