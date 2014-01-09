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

/**
 * Holds metadata for entity column.
 * 
 * @author animesh.kumar
 */
public final class Column
{

    /** name of the column. */
    private String name;

    /** column field. */
    private Field field;

    /** whether indexable. */
    private boolean isIndexable; // default is NOT indexable

    /**
     * Instantiates a new column.
     * 
     * @param name
     *            the name
     * @param field
     *            the field
     */
    public Column(String name, Field field)
    {
        this.name = name;
        this.field = field;
    }

    public Column(String name, Field field, boolean isIndexable)
    {
        this.name = name;
        this.field = field;
        this.isIndexable = isIndexable;
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
     * Gets the field.
     * 
     * @return the field
     */
    public Field getField()
    {
        return field;
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

}