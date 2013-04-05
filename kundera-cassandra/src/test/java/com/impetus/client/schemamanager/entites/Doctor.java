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
package com.impetus.client.schemamanager.entites;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Kuldeep Mishra
 * @version 1.0
 * 
 */
@Entity
@Table(name = "DOCTOR", schema = "KunderaExamples@secIdxCassandraTest")
@IndexCollection(columns = { @Index(name = "age"), @Index(name = "key") })
public class Doctor
{
    @Id
    private String id;

    @Column
    private String key;

    @Column
    private long age;

    /**
     * @return the id
     */
    public String getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(String id)
    {
        this.id = id;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return key;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.key = name;
    }

    /**
     * @return the age
     */
    public long getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(long age)
    {
        this.age = age;
    }

}
