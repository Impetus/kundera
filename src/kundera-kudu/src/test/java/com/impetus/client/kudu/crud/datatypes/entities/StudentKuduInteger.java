/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu.crud.datatypes.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class StudentKuduInteger.
 * 
 * @author Devender Yadav
 */
@Entity
@Table(name = "STUDENT_KUDU_INTEGER", schema = "kudutest@dataTypeTest")
public class StudentKuduInteger
{

    /** The id. */
    @Id
    private Integer id;

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
    public Integer getId()
    {
        return id;
    }

    /**
     * Sets the id.
     * 
     * @param id
     *            the id to set
     */
    public void setId(Integer id)
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
