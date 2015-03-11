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
package com.impetus.client.hbase.admin;

/**
 * @author Pragalbh Garg
 * 
 */
public class HBaseCell
{

    private String columnFamily;

    private String columnName;

    private Object value;

    /**
     * Gets the column family.
     * 
     * @return the column family
     */
    public String getColumnFamily()
    {
        return columnFamily;
    }

    /**
     * Sets the column family.
     * 
     * @param columnFamily
     *            the new column family
     */
    public void setColumnFamily(String columnFamily)
    {
        this.columnFamily = columnFamily;
    }

    /**
     * Gets the column name.
     * 
     * @return the column name
     */
    public String getColumnName()
    {
        return columnName;
    }

    /**
     * Sets the column name.
     * 
     * @param columnName
     *            the new column name
     */
    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    /**
     * Gets the value.
     * 
     * @return the value
     */
    public Object getValue()
    {
        return value;
    }

    /**
     * Sets the value.
     * 
     * @param value
     *            the new value
     */
    public void setValue(Object value)
    {
        this.value = value;
    }

    /**
     * Instantiates a new h base cell.
     * 
     * @param columnFamily
     *            the column family
     * @param columnName
     *            the column name
     * @param value
     *            the value
     */
    public HBaseCell(String columnFamily, String columnName, Object value)
    {
        super();
        this.columnFamily = columnFamily;
        this.columnName = columnName;
        this.value = value;
    }

}
