/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
 * @author Amresh Singh
 */
public class JoinTableMetadata
{

    private String joinTableName;

    private String joinTableSchema;

    private Set<String> joinColumns;

    private Set<String> inverseJoinColumns;

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
     * @return the joinTableName
     */
    public String getJoinTableName()
    {
        return joinTableName;
    }

    /**
     * @param joinTableName
     *            the joinTableName to set
     */
    public void setJoinTableName(String joinTableName)
    {
        this.joinTableName = joinTableName;
    }

    /**
     * @return the joinTableSchema
     */
    public String getJoinTableSchema()
    {
        return joinTableSchema;
    }

    /**
     * @param joinTableSchema
     *            the joinTableSchema to set
     */
    public void setJoinTableSchema(String joinTableSchema)
    {
        this.joinTableSchema = joinTableSchema;
    }

    /**
     * @return the joinColumns
     */
    public Set<String> getJoinColumns()
    {
        return joinColumns;
    }

    /**
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
     * @return the inverseJoinColumns
     */
    public Set<String> getInverseJoinColumns()
    {
        return inverseJoinColumns;
    }

    /**
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
