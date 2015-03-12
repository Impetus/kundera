/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.datatypes.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class StudentHBaseFloatPrimitive.
 * 
 * @author Devender Yadav
 */
@Entity
@Table(name = "STUDENT_HBASE_FLOAT_PRIMITIVE", schema = "HBaseNew@dataTypeTest")
public class StudentHBaseFloatPrimitive
{

    /** The id. */
    @Id
    private float id;

    /** The age. */
    @Column(name = "AGE")
    private short age;

    /** The name. */
    @Column(name = "NAME")
    private String name;

    /**
     * Gets the id.
     * 
     * @return the id
     */
    public float getId()
    {
        return id;
    }

    /**
     * Sets the id.
     * 
     * @param id
     *            the id to set
     */
    public void setId(float id)
    {
        this.id = id;
    }

    /**
     * Gets the age.
     * 
     * @return the age
     */
    public short getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     * 
     * @param age
     *            the age to set
     */
    public void setAge(short age)
    {
        this.age = age;
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

}
