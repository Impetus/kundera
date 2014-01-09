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
package com.impetus.kundera.metadata.processor;

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
@NamedQuery(name = "test.named.query", query = "Select t from TestEntity t where t.field = :field")
@NamedQueries({
        @NamedQuery(name = "test.named.queries1", query = "Select t1 from TestEntity t1 where t1.field = :field"),
        @NamedQuery(name = "test.named.queries2", query = "Select t2 from TestEntity t2 where t2.field = :field") })
@NamedNativeQuery(name = "test.native.query", query = "Select native from TestEntity native where native.field = :field")
@NamedNativeQueries({
        @NamedNativeQuery(name = "test.native.query1", query = "Select native1 from TestEntity native1 where native1.field = :field"),
        @NamedNativeQuery(name = "test.native.query2", query = "Select native2 from TestEntity native2 where native2.field = :field") })
public class EntitySample
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
