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
package com.impetus.client.hbase.generatedId.entites;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * The Class HBaseGeneratedIdWithSequenceGenerator.
 * 
 * @author Devender Yadav
 */
@Entity
@Table(name = "HBaseGeneratedIdWithSequenceGenerator", schema = "HBaseNew@autoIdTest")
public class HBaseGeneratedIdWithSequenceGenerator
{

    /** The id. */
    @Id
    @SequenceGenerator(name = "id_gen", allocationSize = 20, initialValue = 80, schema = "HBaseNew", sequenceName = "newSequence")
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.SEQUENCE)
    private int id;

    /** The name. */
    @Column
    private String name;

    /**
     * Gets the id.
     * 
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * Sets the id.
     * 
     * @param id
     *            the id to set
     */
    public void setId(int id)
    {
        this.id = id;
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