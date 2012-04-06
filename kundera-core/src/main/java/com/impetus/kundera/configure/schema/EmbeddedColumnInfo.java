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

import java.util.List;

/**
 * The Class ColumnInfo holds the information of Embedded Columns.
 * 
 * @author Kuldeep.Kumar
 * 
 */
public class EmbeddedColumnInfo
{
    /** The embedded column name variable . */
    private String embeddedColumnName;

    /** The list of columns variable is columns . */
    private List<ColumnInfo> columns;

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
     * @return the columns
     */
    public List<ColumnInfo> getColumns()
    {
        return columns;
    }

    /**
     * @param columns
     *            the columns to set
     */
    public void setColumns(List<ColumnInfo> columns)
    {
        this.columns = columns;
    }

    /**
     * Equals method compare two object of EmbeddedColumnInfo on the basis of
     * their name.
     * 
     * @param Object
     *            instance.
     * 
     * @return boolean value.
     */
    @Override
    public boolean equals(Object obj)
    {
        boolean result = false;
        if (obj == null)
        {
            result = false;
        }
        else if (getClass() != obj.getClass())
        {
            result = false;
        }
        else
        {
            EmbeddedColumnInfo embeddedColumnInfo = (EmbeddedColumnInfo) obj;

            if (this.embeddedColumnName != null
                    && this.embeddedColumnName.equals(embeddedColumnInfo.embeddedColumnName))
            {

                result = true;
            }
        }
        return result;
    }

}
