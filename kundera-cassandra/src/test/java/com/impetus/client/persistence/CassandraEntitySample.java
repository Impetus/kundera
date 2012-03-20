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
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

/**
 * Test Entity
 * 
 * @author vivek.mishra
 * 
 */

@Entity
@Table(name = "table", schema = "testSchema")
public class CassandraEntitySample
{

    @Id
    private Integer key;

    @Column(name = "field")
    private String field;

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

}
