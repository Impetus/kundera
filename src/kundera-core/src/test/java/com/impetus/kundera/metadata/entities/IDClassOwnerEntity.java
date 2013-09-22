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
import javax.persistence.IdClass;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 * 
 */
@IdClass(value = IDClassEntity.class)
@Entity
@Table(name = "table", schema = "testSchema@keyspace")
public class IDClassOwnerEntity
{

    @Id
    private String id;

    @Column
    private long logColumn;

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
     * @return the logColumn
     */
    public long getLogColumn()
    {
        return logColumn;
    }

    /**
     * @param logColumn
     *            the logColumn to set
     */
    public void setLogColumn(long logColumn)
    {
        this.logColumn = logColumn;
    }

}
