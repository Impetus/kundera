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
package com.impetus.kundera.metadata.model;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;

/**
 * The Class JoinTableMetadata.
 * 
 * @author Amresh Singh
 */
public class JoinTableMetadata
{

    /** The join table name. */
    private String joinTableName;

    /** The join table schema. */
    private String joinTableSchema;

    /** The join columns. */
    private Set<String> joinColumns;

    /** The inverse join columns. */
    private Set<String> inverseJoinColumns;

    /**
     * Instantiates a new join table metadata.
     * 
     * @param relationField
     *            the relation field
     */
    public JoinTableMetadata(Field relationField)
    {
        JoinTable jtAnn = relationField.getAnnotation(JoinTable.class);

        setJoinTableName(jtAnn.name());
        setJoinTableSchema(jtAnn.schema());

        for (JoinColumn joinColumn : jtAnn.joinColumns())
        {
            addJoinColumns(joinColumn.name());
        }

        for (JoinColumn inverseJoinColumn : jtAnn.inverseJoinColumns())
        {
            addInverseJoinColumns(inverseJoinColumn.name());
        }
    }

    /**
     * Gets the join table name.
     * 
     * @return the joinTableName
     */
    public String getJoinTableName()
    {
        return joinTableName;
    }

    /**
     * Sets the join table name.
     * 
     * @param joinTableName
     *            the joinTableName to set
     */
    public void setJoinTableName(String joinTableName)
    {
        this.joinTableName = joinTableName;
    }

    /**
     * Gets the join table schema.
     * 
     * @return the joinTableSchema
     */
    public String getJoinTableSchema()
    {
        return joinTableSchema;
    }

    /**
     * Sets the join table schema.
     * 
     * @param joinTableSchema
     *            the joinTableSchema to set
     */
    public void setJoinTableSchema(String joinTableSchema)
    {
        this.joinTableSchema = joinTableSchema;
    }

    /**
     * Gets the join columns.
     * 
     * @return the joinColumns
     */
    public Set<String> getJoinColumns()
    {
        return joinColumns;
    }

    /**
     * Adds the join columns.
     * 
     * @param joinColumn
     *            the joinColumns to add
     */
    public void addJoinColumns(String joinColumn)
    {
        if (joinColumns == null || joinColumns.isEmpty())
        {
            joinColumns = new HashSet<String>();
        }
        joinColumns.add(joinColumn);
    }

    /**
     * Gets the inverse join columns.
     * 
     * @return the inverseJoinColumns
     */
    public Set<String> getInverseJoinColumns()
    {
        return inverseJoinColumns;
    }

    /**
     * Adds the inverse join columns.
     * 
     * @param inverseJoinColumn
     *            the inverseJoinColumns to add
     */
    public void addInverseJoinColumns(String inverseJoinColumn)
    {
        if (inverseJoinColumns == null || inverseJoinColumns.isEmpty())
        {
            inverseJoinColumns = new HashSet<String>();
        }

        inverseJoinColumns.add(inverseJoinColumn);
    }

}
