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
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;

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
    @ElementCollection
    private Map<String, Address> addresses;

    /** The phones. */
    @ElementCollection
    private List<Phone> phones;
    
    /** The spouses. */
    @ElementCollection
    private Set<Spouse> spouses;

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
    public Map<String, Address> getAddresses()
    {
        return addresses;
    }

    /**
     * Sets the addresses.
     *
     * @param addresses the addresses
     */
    public void setAddresses(Map<String, Address> addresses)
    {
        this.addresses = addresses;
    }

    /**
     * Gets the phones.
     *
     * @return the phones
     */
    public List<Phone> getPhones()
    {
        return phones;
    }

    /**
     * Sets the phones.
     *
     * @param phones the new phones
     */
    public void setPhones(List<Phone> phones)
    {
        this.phones = phones;
    }

    /**
     * Gets the spouses.
     *
     * @return the spouses
     */
    public Set<Spouse> getSpouses()
    {
        return spouses;
    }

    /**
     * Sets the spouses.
     *
     * @param spouses the new spouses
     */
    public void setSpouses(Set<Spouse> spouses)
    {
        this.spouses = spouses;
    }
    
    

}
