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

package com.impetus.kundera.configure.schema;

/**
 * The Class ColumnInfo holds column related information.
 * 
 * @author Kuldeep.Kumar
 */
public class ColumnInfo
{

    /** The is indexable variable for indexing the column. */
    private boolean isIndexable = false;

    /** The column name variable . */
    private String columnName;

    /** The type variable. */
    private String type;

    /**
     * Instantiates a new column info.
     */
    public ColumnInfo()
    {

    }

    /**
     * Equals method compare two object of columnInfo on the basis of their
     * name.
     * 
     * @param Object
     *            instance.
     * 
     * @return boolean value.
     */
    @Override
    public boolean equals(Object obj)
    {

        // / if object's class and column name matches then return true;

        return obj != null && obj instanceof ColumnInfo && ((ColumnInfo) obj).columnName != null ? this.columnName != null
                && this.columnName.equals(((ColumnInfo) obj).columnName)
                && this.isIndexable == ((ColumnInfo) obj).isIndexable
                : false;

    }

    /**
     * Gets the column name.
     * 
     * @return the columnName
     */
    public String getColumnName()
    {
        return columnName;
    }

    /**
     * Sets the column name.
     * 
     * @param columnName
     *            the columnName to set
     */
    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    /**
     * Checks if is indexable.
     * 
     * @return the isIndexable
     */
    public boolean isIndexable()
    {
        return isIndexable;
    }

    /**
     * Sets the indexable.
     * 
     * @param isIndexable
     *            the isIndexable to set
     */
    public void setIndexable(boolean isIndexable)
    {
        this.isIndexable = isIndexable;
    }

    /**
     * Gets the type.
     * 
     * @return the type
     */
    public String getType()
    {
        return type;
    }

    /**
     * Sets the type.
     * 
     * @param type
     *            the type to set
     */
    public void setType(String type)
    {
        this.type = type;
    }
}
