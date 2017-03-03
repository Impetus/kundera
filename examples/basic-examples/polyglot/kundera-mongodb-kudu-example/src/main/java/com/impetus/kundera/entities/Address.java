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
package com.impetus.kundera.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class Address.
 */
@Entity
@Table(name = "ADDRESS", schema = "kundera_mongo@mongo_pu")
public class Address
{

    /** The address id. */
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    /** The street. */
    @Column(name = "STREET")
    private String street;

    /** The city. */
    @Column(name = "CITY")
    private String city;

    /** The country. */
    @Column(name = "COUNTRY")
    private String country;

    /**
     * Instantiates a new address.
     */
    public Address()
    {

    }

    /**
     * Instantiates a new address.
     * 
     * @param addressId
     *            the address id
     * @param street
     *            the street
     * @param city
     *            the city
     * @param country
     *            the country
     */
    public Address(String addressId, String street, String city, String country)
    {
        this.addressId = addressId;
        this.street = street;
        this.city = city;
        this.country = country;
    }

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

    /**
     * Gets the country.
     * 
     * @return the country
     */
    public String getCountry()
    {
        return country;
    }

    /**
     * Sets the country.
     * 
     * @param country
     *            the new country
     */
    public void setCountry(String country)
    {
        this.country = country;
    }

}
