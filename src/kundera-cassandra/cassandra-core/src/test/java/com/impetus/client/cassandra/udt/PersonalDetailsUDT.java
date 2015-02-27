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
 /*
 * author: karthikp.manchala
 */
package com.impetus.client.cassandra.udt;

import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;

// TODO: Auto-generated Javadoc
/**
 * The Class PersonalDetailsUDT.
 */
@Embeddable
public class PersonalDetailsUDT
{

    /** The fullname. */
    @Embedded
    private Fullname fullname;

    /** The addresses. */
    @Embedded
    private Address addresses;

    /** The phone. */
    @Embedded
    private Phone phone;

    /** The spouse. */
    @Embedded
    private Spouse spouse;

    /**
     * Gets the fullname.
     *
     * @return the fullname
     */
    public Fullname getFullname()
    {
        return fullname;
    }

    /**
     * Sets the fullname.
     *
     * @param fullname the new fullname
     */
    public void setFullname(Fullname fullname)
    {
        this.fullname = fullname;
    }

    /**
     * Gets the addresses.
     *
     * @return the addresses
     */
    public Address getAddresses()
    {
        return addresses;
    }

    /**
     * Sets the addresses.
     *
     * @param addresses the new addresses
     */
    public void setAddresses(Address addresses)
    {
        this.addresses = addresses;
    }

    /**
     * Gets the phone.
     *
     * @return the phone
     */
    public Phone getPhone()
    {
        return phone;
    }

    /**
     * Sets the phone.
     *
     * @param phone the new phone
     */
    public void setPhone(Phone phone)
    {
        this.phone = phone;
    }

    /**
     * Gets the spouse.
     *
     * @return the spouse
     */
    public Spouse getSpouse()
    {
        return spouse;
    }

    /**
     * Sets the spouse.
     *
     * @param spouse the new spouse
     */
    public void setSpouse(Spouse spouse)
    {
        this.spouse = spouse;
    }

}
