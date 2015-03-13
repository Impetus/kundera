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

import java.util.List;

import javax.persistence.CollectionTable;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;

/**
 * The Class PersonalDetails.
 * 
 * @author Pragalbh Garg
 */
@Embeddable
public class PersonalDetails
{

    /** The fullname. */
    @Embedded
    private Fullname fullname;

    /** The addresses. */
    @ElementCollection
    @CollectionTable(name = "add")
    private List<Address> addresses;

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
     * @param fullname
     *            the new fullname
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
    public List<Address> getAddresses()
    {
        return addresses;
    }

    /**
     * Sets the addresses.
     * 
     * @param addresses
     *            the new addresses
     */
    public void setAddresses(List<Address> addresses)
    {
        this.addresses = addresses;
    }

}
