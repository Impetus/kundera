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

import javax.persistence.metamodel.EmbeddableType;

import org.apache.commons.lang.builder.HashCodeBuilder;

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

    private EmbeddableType embeddable;

    /**
     * @param metaModel
     */
    public EmbeddedColumnInfo(EmbeddableType metaModel)
    {
        this.embeddable = metaModel;
    }

    /**
     * @return the embeddable
     */
    public EmbeddableType getEmbeddable()
    {
        return embeddable;
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
        return obj != null && obj instanceof EmbeddedColumnInfo
                && ((EmbeddedColumnInfo) obj).embeddedColumnName != null ? this.embeddedColumnName != null
                && this.embeddedColumnName.equals(((EmbeddedColumnInfo) obj).embeddedColumnName) : false;
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
        StringBuilder strBuilder = new StringBuilder("embeddedColumnName:==> ");
        strBuilder.append(embeddedColumnName);
        return strBuilder.toString();
    }
}
