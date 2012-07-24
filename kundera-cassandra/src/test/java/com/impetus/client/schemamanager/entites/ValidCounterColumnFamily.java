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

@Entity
@Table(name = "ValidCounterColumnFamily", schema = "KunderaCounterColumn@cassandraProperties")
public class ValidCounterColumnFamily
{

    @Id
    private int id;

    @Column
    private long counter;

    @Column
    private Long lCounter;

    /**
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(int id)
    {
        this.id = id;
    }

    /**
     * @return the counter
     */
    public long getCounter()
    {
        return counter;
    }

    /**
     * @param counter
     *            the counter to set
     */
    public void setCounter(long counter)
    {
        this.counter = counter;
    }

    /**
     * @return the lCounter
     */
    public Long getlCounter()
    {
        return lCounter;
    }

    /**
     * @param lCounter
     *            the lCounter to set
     */
    public void setlCounter(Long lCounter)
    {
        this.lCounter = lCounter;
    }

}
