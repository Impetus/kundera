/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.oraclenosql.schemamanager;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * The Class OracleNoSQLUser.
 * 
 * @author devender.yadav
 */
@Entity
@Table(name = "ONS_USER")
@IndexCollection(columns = { @Index(indexName = "name", name = "name"),
        @Index(indexName = "age", name = "age")
        ,@Index(indexName = "email", name = "details.email", type = "composite"),
        @Index(indexName = "address", name = "details.address", type = "composite")
})
public class OracleNoSQLUser
{
    /** The user id. */
    @Id
    @Column(name = "userId")
    private int userId;

    /** The name. */
    @Column(name = "NAME")
    private String name;

    /** The age. */
    @Column(name = "age")
    private int age;

    @Embedded
    private UserDetails details;

    /**
     * Gets the user id.
     * 
     * @return the user id
     */
    public int getUserId()
    {
        return userId;
    }

    /**
     * Sets the user id.
     * 
     * @param userId
     *            the new user id
     */
    public void setUserId(int userId)
    {
        this.userId = userId;
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
     *            the new name
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Gets the age.
     * 
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * Gets the details.
     * 
     * @return the details
     */
    public UserDetails getDetails()
    {
        return details;
    }

    public void setDetails(UserDetails details)
    {
        this.details = details;
    }

}
