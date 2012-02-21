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
import java.util.ArrayList;
import java.util.List;

/**
 * Holds metadata for embedded column in entity.
 *
 * @author animesh.kumar
 */
public final class EmbeddedColumn
{

    /** The name. */
    private String name;

    /** Super column field. */
    private Field field;

    /** The columns. */
    private List<Column> columns;

    /**
     * Instantiates a new super column.
     * 
     * @param name
     *            the name
     * @param f
     *            the f
     */
    public EmbeddedColumn(String name, Field f)
    {
        this.name = name;
        this.field = f;
        columns = new ArrayList<Column>();
    }

    /**
     * Gets the name.
     * 
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     * 
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Gets the field.
     * 
     * @return the field
     */
    public Field getField()
    {
        return field;
    }

    /**
     * Sets the field.
     * 
     * @param field
     *            the field to set
     */
    public void setField(Field field)
    {
        this.field = field;
    }

    /**
     * Gets the columns.
     * 
     * @return the columns
     */
    public List<Column> getColumns()
    {
        return columns;
    }

    /**
     * Adds the column.
     * 
     * @param name
     *            the name
     * @param field
     *            the field
     */
    public void addColumn(String name, Field field)
    {
        columns.add(new Column(name, field));
    }
}