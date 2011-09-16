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
package com.impetus.kundera.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that represents a row in Column family based datastores like cassandra and HBase. 
 * @author amresh.singh
 */
public class DataRow<TF>
{
    /** Id of the row. */
    private String id;

    /** name of the family. */
    private String columnFamilyName;

    /** list of thrift columns from the row. */
    private List<TF> columns;

    /**
     * default constructor.
     */
    public DataRow()
    {
        columns = new ArrayList<TF>();
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
     */
    public DataRow(String id, String columnFamilyName, List<TF> columns)
    {
        this.id = id;
        this.columnFamilyName = columnFamilyName;
        this.columns = columns;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId()
    {
        return id;
    }

    /**
     * Sets the id.
     *
     * @param id
     *            the key to set
     */
    public void setId(String id)
    {
        this.id = id;
    }

    /**
     * Gets the column family name.
     *
     * @return the columnFamilyName
     */
    public String getColumnFamilyName()
    {
        return columnFamilyName;
    }

    /**
     * Sets the column family name.
     *
     * @param columnFamilyName
     *            the columnFamilyName to set
     */
    public void setColumnFamilyName(String columnFamilyName)
    {
        this.columnFamilyName = columnFamilyName;
    }

    /**
     * Gets the columns.
     *
     * @return the columns
     */
    public List<TF> getColumns()
    {
        return columns;
    }

    /**
     * Sets the columns.
     *
     * @param columns
     *            the columns to set
     */
    public void setColumns(List<TF> columns)
    {
        this.columns = columns;
    }

    /**
     * Adds the column.;
     *
     * @param column
     *            the column
     */
    public void addColumn(TF column)
    {
        columns.add(column);
    }
}
