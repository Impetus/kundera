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
package com.impetus.client.hbase.crud.embedded;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * @author Pragalbh Garg
 * 
 */
@Embeddable
public class Address
{

    /** The street. */
    @Column(name = "street")
    private String street;

    /** The city. */
    @Column(name = "city")
    private String city;

    /** The pin. */
    @Column(name = "pin")
    private String pin;

    /**
     * Instantiates a new address.
     */
    public Address()
    {

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
     * Gets the pin.
     * 
     * @return the pin
     */
    public String getPin()
    {
        return pin;
    }

    /**
     * Sets the pin.
     * 
     * @param pin
     *            the new pin
     */
    public void setPin(String pin)
    {
        this.pin = pin;
    }

}
