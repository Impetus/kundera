/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.kundera.dataasobject.entities;

import javax.persistence.Entity;
import javax.persistence.Id;

import com.impetus.core.DefaultKunderaEntity;

/**
 * The Class Customer.
 */
@Entity
public class Customer extends DefaultKunderaEntity<Customer, Integer>
{

    /** The customer id. */
    @Id
    private int customerId;

    /** The name. */
    private String name;

    /** The location. */
    private String location;

    /**
     * Gets the customer id.
     *
     * @return the customer id
     */
    public int getCustomerId()
    {
        return customerId;
    }

    /**
     * Sets the customer id.
     *
     * @param customerId
     *            the new customer id
     */
    public void setCustomerId(int customerId)
    {
        this.customerId = customerId;
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
     * Gets the location.
     *
     * @return the location
     */
    public String getLocation()
    {
        return location;
    }

    /**
     * Sets the location.
     *
     * @param location
     *            the new location
     */
    public void setLocation(String location)
    {
        this.location = location;
    }

}
