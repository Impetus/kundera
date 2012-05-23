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
package com.impetus.client.persistence;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Test Entity
 * 
 * @author vivek.mishra
 * 
 */

@Entity
@Table(name = "users", schema = "cassandra@KunderaExamples")
// @NamedQuery(name="delete.query",query="Delete From CassandraEntitySample c where c.state=UP")
public class CassandraEntitySample
{

    @Id
    @Column(name = "key")
    private String key;

    @Column(name = "full_name")
    private String full_name;

    @Column(name = "birth_date")
    private Integer birth_date;

    @Column(name = "state")
    private String state;

    /**
     * @return the key
     */
    public String getKey()
    {
        return key;
    }

    /**
     * @param key
     *            the key to set
     */
    public void setKey(String key)
    {
        this.key = key;
    }

    /**
     * @return the full_name
     */
    public String getFull_name()
    {
        return full_name;
    }

    /**
     * @param full_name
     *            the full_name to set
     */
    public void setFull_name(String full_name)
    {
        this.full_name = full_name;
    }

    /**
     * @return the birth_date
     */
    public Integer getBirth_date()
    {
        return birth_date;
    }

    /**
     * @param birth_date
     *            the birth_date to set
     */
    public void setBirth_date(int birth_date)
    {
        this.birth_date = birth_date;
    }

    /**
     * @return the state
     */
    public String getState()
    {
        return state;
    }

    /**
     * @param state
     *            the state to set
     */
    public void setState(String state)
    {
        this.state = state;
    }

}
