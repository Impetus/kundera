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
    private EntityMetadata m;

    private boolean isIdQuery;

    private int limit;

    private Object keyValue;

    private String keyName;

    private Map<String, Object> keyValues;

    private Object startKeyValue;

    private Object endKeyValue;

    private boolean includeDocs = true;

    private boolean descending = false;

    private String[] columns;

    private boolean includeLastKey = true;

    private String operator;

    public CouchDBQueryInterpreter(String[] columns, int limit, EntityMetadata m)
    {
        this.m = m;
        this.columns = columns;
        this.limit = limit;
    }

    public int getLimit()
    {
        return limit;
    }

    public void setLimit(int limit)
    {
        this.limit = limit;
    }

    public Object getKeyValue()
    {
        return keyValue;
    }

    public void setKeyValue(Object keyValue)
    {
        this.keyValue = keyValue;
    }

    public Object getStartKeyValue()
    {
        return startKeyValue;
    }

    public void setStartKeyValue(Object startKeyValue)
    {
        this.startKeyValue = startKeyValue;
    }

    public Object getEndKeyValue()
    {
        return endKeyValue;
    }

    public void setEndKeyValue(Object endKeyValue)
    {
        this.endKeyValue = endKeyValue;
    }

    public boolean isIncludeDocs()
    {
        return includeDocs;
    }

    public void setIncludeDocs(boolean includeDocs)
    {
        this.includeDocs = includeDocs;
    }

    public boolean isDescending()
    {
        return descending;
    }

    public void setDescending(boolean descending)
    {
        this.descending = descending;
    }

    public boolean isIdQuery()
    {
        return isIdQuery;
    }

    public void setIdQuery(boolean isIdQuery)
    {
        this.isIdQuery = isIdQuery;
    }

    public String[] getColumns()
    {
        return columns;
    }

    public void setColumns(String[] columns)
    {
        this.columns = columns;
    }

    public Map<String, Object> getKeyValues()
    {
        return this.keyValues;
    }

    public void setKeyValues(String keyName, Object obj)
    {
        if (this.keyValues == null)
        {
            this.keyValues = new HashMap<String, Object>();
        }
        this.keyValues.put(keyName, obj);
    }

    public boolean isIncludeLastKey()
    {
        return includeLastKey;
    }

    public void setIncludeLastKey(boolean includeLastKey)
    {
        this.includeLastKey = includeLastKey;
    }

    public EntityMetadata getMetadata()
    {
        return m;
    }

    public boolean isRangeQuery()
    {
        return getStartKeyValue() != null && getEndKeyValue() != null;
    }

    public String getOperator()
    {
        return this.operator;
    }

    public void setOperator(String operator)
    {
        this.operator = operator;
    }

    public String getKeyName()
    {
        return keyName;
    }

    public void setKeyName(String keyName)
    {
        this.keyName = keyName;
    }
}