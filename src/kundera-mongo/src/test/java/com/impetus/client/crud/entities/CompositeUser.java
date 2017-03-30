/*******************************************************************************
 * * Copyright 2017 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.client.crud.entities;

import javax.persistence.*;

/**
 * The Class CompositeUser.
 */
@Entity
@Table(name = "composite_user")
public class CompositeUser
{

    /** The id. */
    @EmbeddedId
    private CompositeId id;

    /** The phone. */
    @Basic
    @Column(name = "phone")
    private String phone;

    /**
     * Gets the id.
     * 
     * @return the id
     */
    public CompositeId getId()
    {
        return id;
    }

    /**
     * Sets the id.
     * 
     * @param id
     *            the new id
     */
    public void setId(final CompositeId id)
    {
        this.id = id;
    }

    /**
     * Gets the phone.
     * 
     * @return the phone
     */
    public String getPhone()
    {
        return phone;
    }

    /**
     * Sets the phone.
     * 
     * @param phone
     *            the new phone
     */
    public void setPhone(final String phone)
    {
        this.phone = phone;
    }

}
