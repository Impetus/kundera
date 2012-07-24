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
package com.impetus.kundera.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds High level search result info from database records
 * 
 * @author amresh
 * 
 */
public class SearchResult
{
    private Object primaryKey;

    private String embeddedColumnName;

    private List<String> embeddedColumnValues;

    /**
     * @return the primaryKey
     */
    public Object getPrimaryKey()
    {
        return primaryKey;
    }

    /**
     * @param primaryKey
     *            the primaryKey to set
     */
    public void setPrimaryKey(Object primaryKey)
    {
        this.primaryKey = primaryKey;
    }

    /**
     * @return the embeddedColumnName
     */
    public String getEmbeddedColumnName()
    {
        return embeddedColumnName;
    }

    /**
     * @param embeddedColumnName
     *            the embeddedColumnName to set
     */
    public void setEmbeddedColumnName(String embeddedColumnName)
    {
        this.embeddedColumnName = embeddedColumnName;
    }

    /**
     * @return the embeddedColumnValues
     */
    public List<String> getEmbeddedColumnValues()
    {
        return embeddedColumnValues;
    }

    /**
     * @param embeddedColumnValues
     *            the embeddedColumnValues to set
     */
    public void addEmbeddedColumnValue(String embeddedColumnValue)
    {
        if (embeddedColumnValues == null)
        {
            embeddedColumnValues = new ArrayList<String>();
        }
        embeddedColumnValues.add(embeddedColumnValue);
    }

}
