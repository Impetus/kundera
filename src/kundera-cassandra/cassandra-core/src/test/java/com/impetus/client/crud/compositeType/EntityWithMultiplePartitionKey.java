/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.crud.compositeType;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * 
 * @author Kuldeep.Mishra
 * 
 */
@Entity
@Table(name = "EntityWithMultiplePartitionKey")
public class EntityWithMultiplePartitionKey
{
    @EmbeddedId
    private IdWithMultiplePartitionKey id;

    @Column
    private String entityDiscription;

    @Column
    private String action;

    /**
     * @return the id
     */
    public IdWithMultiplePartitionKey getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(IdWithMultiplePartitionKey id)
    {
        this.id = id;
    }

    /**
     * @return the entityDiscription
     */
    public String getEntityDiscription()
    {
        return entityDiscription;
    }

    /**
     * @param entityDiscription
     *            the entityDiscription to set
     */
    public void setEntityDiscription(String entityDiscription)
    {
        this.entityDiscription = entityDiscription;
    }

    /**
     * @return the action
     */
    public String getAction()
    {
        return action;
    }

    /**
     * @param action
     *            the action to set
     */
    public void setAction(String action)
    {
        this.action = action;
    }

}
