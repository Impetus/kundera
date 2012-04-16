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

import org.apache.commons.lang.builder.HashCodeBuilder;

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
    private Class type;

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
                : false;

    }

    @Override
    /**
     * returns the hash code for object. 
     * 
     */
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    /**
     * returns the string representation of object .
     * 
     */
    public String toString()
    {
        StringBuilder strBuilder = new StringBuilder("type:==> ");
        strBuilder.append(type);
        strBuilder.append(" | columnName: ==>");
        strBuilder.append(columnName);
        strBuilder.append(" | isIndexable: ==>");
        strBuilder.append(isIndexable);
        return strBuilder.toString();
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
     * @return the type
     */
    public Class getType()
    {
        return type;
    }

    /**
     * @param type
     *            the type to set
     */
    public void setType(Class type)
    {
        this.type = type;
    }
}
