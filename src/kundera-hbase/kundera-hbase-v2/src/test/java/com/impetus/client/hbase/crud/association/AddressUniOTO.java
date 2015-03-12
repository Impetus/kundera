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
package com.impetus.client.hbase.crud.association;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class AddressUniOTO.
 * 
 * @author Devender Yadav
 */
@Entity
@Table(name = "ADDRESS_UOTO", schema = "HBaseNew@associationTest")
public class AddressUniOTO
{

    /** The address id. */
    @Id
    @Column(name = "ADDRESS_ID")
    private int addressId;

    /** The city. */
    @Column(name = "CITY")
    private String city;

    /**
     * Instantiates a new address uni oto.
     */
    public AddressUniOTO()
    {

    }

    /**
     * Instantiates a new address uni oto.
     * 
     * @param addressId
     *            the address id
     * @param city
     *            the city
     */
    public AddressUniOTO(int addressId, String city)
    {
        this.addressId = addressId;
        this.city = city;
    }

    /**
     * Gets the address id.
     * 
     * @return the address id
     */
    public int getAddressId()
    {
        return addressId;
    }

    /**
     * Sets the address id.
     * 
     * @param addressId
     *            the new address id
     */
    public void setAddressId(int addressId)
    {
        this.addressId = addressId;
    }

    /**
     * Gets the city.
     * 
     * @return the city
     */
    public String getCity()
    {
        return city;
    }

    /**
     * Sets the city.
     * 
     * @param city
     *            the new city
     */
    public void setCity(String city)
    {
        this.city = city;
    }

}
