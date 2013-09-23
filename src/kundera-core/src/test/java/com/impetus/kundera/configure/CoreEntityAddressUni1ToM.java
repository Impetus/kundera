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

package com.impetus.kundera.configure;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class CoreEntityAddressUni1ToM.
 */
@Entity
@Table(name = "CoreEntityAddressUni1ToM", schema = "KunderaCoreExmples@cassandra")
public class CoreEntityAddressUni1ToM
{

    /** The address id. */
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    /** The street. */
    @Column(name = "STREET")
    private String street;

    /**
     * Gets the address id.
     * 
     * @return the address id
     */
    public String getAddressId()
    {
        return addressId;
    }

    /**
     * Sets the address id.
     * 
     * @param addressId
     *            the new address id
     */
    public void setAddressId(String addressId)
    {
        this.addressId = addressId;
    }

    /**
     * Gets the street.
     * 
     * @return the street
     */
    public String getStreet()
    {
        return street;
    }

    /**
     * Sets the street.
     * 
     * @param street
     *            the new street
     */
    public void setStreet(String street)
    {
        this.street = street;
    }

}
