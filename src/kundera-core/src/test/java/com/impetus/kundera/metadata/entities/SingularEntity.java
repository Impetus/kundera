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
package com.impetus.kundera.metadata.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.Table;

/**
 * Entity with singular attributes for meta model processing.
 * 
 * @author vivek.mishra
 * 
 */

@Entity
@Table(name = "table", schema = "testSchema@keyspace")
@Inheritance(strategy=InheritanceType.TABLE_PER_CLASS)
public class SingularEntity
{

    @Id
    private Integer key;

    @Column(name = "field", nullable = false)
    private String field;

    @Column(name = "name")
    private String name;

    /**
     * @return the key
     */
    public Integer getKey()
    {
        return key;
    }

    /**
     * @param key
     *            the key to set
     */
    public void setKey(Integer key)
    {
        this.key = key;
    }

    /**
     * @return the field
     */
    public String getField()
    {
        return field;
    }

    /**
     * @param field
     *            the field to set
     */
    public void setField(String field)
    {
        this.field = field;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

}
