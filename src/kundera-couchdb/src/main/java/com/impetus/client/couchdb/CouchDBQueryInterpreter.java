/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.couchdb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Query interpreter for CouchDB.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class CouchDBQueryInterpreter
{

    /** The m. */
    private EntityMetadata m;

    /** The is id query. */
    private boolean isIdQuery;

    /** The limit. */
    private int limit;

    /** The key value. */
    private Object keyValue;

    /** The key name. */
    private String keyName;

    /** The key values. */
    private Map<String, Object> keyValues;

    /** The start key value. */
    private Object startKeyValue;

    /** The end key value. */
    private Object endKeyValue;

    /** The include docs. */
    private boolean includeDocs = true;

    /** The descending. */
    private boolean descending = false;

    /** The columns. */
    private String[] columns;

    /** The include last key. */
    private boolean includeLastKey = true;

    /** The operator. */
    private String operator;

    /** The is query on composite key. */
    private boolean isQueryOnCompositeKey = false;

    /** The columns to output. */
    private List<Map<String, Object>> columnsToOutput;

    /** The is aggregation. */
    private boolean isAggregation = false;

    /** The aggregation type. */
    private String aggregationType;

    /** The aggregation column. */
    private String aggregationColumn = null;

    /**
     * Instantiates a new couch db query interpreter.
     * 
     * @param columns
     *            the columns
     * @param limit
     *            the limit
     * @param m
     *            the m
     */
    public CouchDBQueryInterpreter(String[] columns, int limit, EntityMetadata m)
    {
        this.m = m;
        this.columns = columns;
        this.limit = limit;
    }

    /**
     * Gets the limit.
     * 
     * @return the limit
     */
    public int getLimit()
    {
        return limit;
    }

    /**
     * Sets the limit.
     * 
     * @param limit
     *            the new limit
     */
    public void setLimit(int limit)
    {
        this.limit = limit;
    }

    /**
     * Gets the key value.
     * 
     * @return the key value
     */
    public Object getKeyValue()
    {
        return keyValue;
    }

    /**
     * Sets the key value.
     * 
     * @param keyValue
     *            the new key value
     */
    public void setKeyValue(Object keyValue)
    {
        this.keyValue = keyValue;
    }

    /**
     * Gets the start key value.
     * 
     * @return the start key value
     */
    public Object getStartKeyValue()
    {
        return startKeyValue;
    }

    /**
     * Sets the start key value.
     * 
     * @param startKeyValue
     *            the new start key value
     */
    public void setStartKeyValue(Object startKeyValue)
    {
        this.startKeyValue = startKeyValue;
    }

    /**
     * Gets the end key value.
     * 
     * @return the end key value
     */
    public Object getEndKeyValue()
    {
        return endKeyValue;
    }

    /**
     * Sets the end key value.
     * 
     * @param endKeyValue
     *            the new end key value
     */
    public void setEndKeyValue(Object endKeyValue)
    {
        this.endKeyValue = endKeyValue;
    }

    /**
     * Checks if is include docs.
     * 
     * @return true, if is include docs
     */
    public boolean isIncludeDocs()
    {
        return includeDocs;
    }

    /**
     * Sets the include docs.
     * 
     * @param includeDocs
     *            the new include docs
     */
    public void setIncludeDocs(boolean includeDocs)
    {
        this.includeDocs = includeDocs;
    }

    /**
     * Checks if is descending.
     * 
     * @return true, if is descending
     */
    public boolean isDescending()
    {
        return descending;
    }

    /**
     * Sets the descending.
     * 
     * @param descending
     *            the new descending
     */
    public void setDescending(boolean descending)
    {
        this.descending = descending;
    }

    /**
     * Checks if is id query.
     * 
     * @return true, if is id query
     */
    public boolean isIdQuery()
    {
        return isIdQuery;
    }

    /**
     * Sets the id query.
     * 
     * @param isIdQuery
     *            the new id query
     */
    public void setIdQuery(boolean isIdQuery)
    {
        this.isIdQuery = isIdQuery;
    }

    /**
     * Gets the columns.
     * 
     * @return the columns
     */
    public String[] getColumns()
    {
        return columns;
    }

    /**
     * Sets the columns.
     * 
     * @param columns
     *            the new columns
     */
    public void setColumns(String[] columns)
    {
        this.columns = columns;
    }

    /**
     * Gets the key values.
     * 
     * @return the key values
     */
    public Map<String, Object> getKeyValues()
    {
        return this.keyValues;
    }

    /**
     * Sets the key values.
     * 
     * @param keyName
     *            the key name
     * @param obj
     *            the obj
     */
    public void setKeyValues(String keyName, Object obj)
    {
        if (this.keyValues == null)
        {
            this.keyValues = new HashMap<String, Object>();
        }
        this.keyValues.put(keyName, obj);
    }

    /**
     * Checks if is include last key.
     * 
     * @return true, if is include last key
     */
    public boolean isIncludeLastKey()
    {
        return includeLastKey;
    }

    /**
     * Sets the include last key.
     * 
     * @param includeLastKey
     *            the new include last key
     */
    public void setIncludeLastKey(boolean includeLastKey)
    {
        this.includeLastKey = includeLastKey;
    }

    /**
     * Gets the metadata.
     * 
     * @return the metadata
     */
    public EntityMetadata getMetadata()
    {
        return m;
    }

    /**
     * Checks if is range query.
     * 
     * @return true, if is range query
     */
    public boolean isRangeQuery()
    {
        return getStartKeyValue() != null && getEndKeyValue() != null;
    }

    /**
     * Gets the operator.
     * 
     * @return the operator
     */
    public String getOperator()
    {
        return this.operator;
    }

    /**
     * Sets the operator.
     * 
     * @param operator
     *            the new operator
     */
    public void setOperator(String operator)
    {
        this.operator = operator;
    }

    /**
     * Gets the key name.
     * 
     * @return the key name
     */
    public String getKeyName()
    {
        return keyName;
    }

    /**
     * Sets the key name.
     * 
     * @param keyName
     *            the new key name
     */
    public void setKeyName(String keyName)
    {
        this.keyName = keyName;
    }

    /**
     * Checks if is query on composite key.
     * 
     * @return true, if is query on composite key
     */
    public boolean isQueryOnCompositeKey()
    {
        return isQueryOnCompositeKey;
    }

    /**
     * Sets the query on composite key.
     * 
     * @param isQueryOnCompositeKey
     *            the new query on composite key
     */
    public void setQueryOnCompositeKey(boolean isQueryOnCompositeKey)
    {
        this.isQueryOnCompositeKey = isQueryOnCompositeKey;
    }

    /**
     * Gets the columns to output.
     * 
     * @return the columns to output
     */
    public List<Map<String, Object>> getColumnsToOutput()
    {
        return columnsToOutput;
    }

    /**
     * Sets the columns to output.
     * 
     * @param columnsToOutput
     *            the columns to output
     */
    public void setColumnsToOutput(List<Map<String, Object>> columnsToOutput)
    {
        this.columnsToOutput = columnsToOutput;
    }

    /**
     * Checks if is aggregation.
     * 
     * @return true, if is aggregation
     */
    public boolean isAggregation()
    {
        return isAggregation;
    }

    /**
     * Sets the aggregation.
     * 
     * @param isAggregation
     *            the new aggregation
     */
    public void setAggregation(boolean isAggregation)
    {
        this.isAggregation = isAggregation;
    }

    /**
     * Gets the aggregation type.
     * 
     * @return the aggregation type
     */
    public String getAggregationType()
    {
        return aggregationType;
    }

    /**
     * Sets the aggregation type.
     * 
     * @param type
     *            the new aggregation type
     */
    public void setAggregationType(String type)
    {
        this.aggregationType = type;
    }

    /**
     * Gets the aggregation column.
     * 
     * @return the aggregation column
     */
    public String getAggregationColumn()
    {
        return aggregationColumn;
    }

    /**
     * Sets the aggregation column.
     * 
     * @param aggregationColumn
     *            the new aggregation column
     */
    public void setAggregationColumn(String aggregationColumn)
    {
        this.aggregationColumn = aggregationColumn;
    }
}