/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.secondarytable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class PersonSecondaryTableAddress.
 * 
 * @author Pragalbh Garg
 */
@Entity
@Table(name = "SEC_TABLE", schema = "HBaseNew@secTableTest")
public class PersonSecondaryTableAddress
{

    /** The address id. */
    @Id
    @Column(name = "ADDRESS_ID")
    private Double addressId;

    /**
     * Instantiates a new person secondary table address.
     */
    public PersonSecondaryTableAddress()
    {

    }

    /**
     * Instantiates a new person secondary table address.
     * 
     * @param addressId
     *            the address id
     */
    public PersonSecondaryTableAddress(Double addressId)
    {
        this.addressId = addressId;
    }

    /** The address. */
    @Column(name = "address")
    private String address;

    /**
     * Gets the address.
     * 
     * @return the address
     */
    public String getAddress()
    {
        return address;
    }

    /**
     * Sets the address.
     * 
     * @param address
     *            the new address
     */
    public void setAddress(String address)
    {
        this.address = address;
    }

    /**
     * Gets the address id.
     * 
     * @return the address id
     */
    public Double getAddressId()
    {
        return addressId;
    }

}
