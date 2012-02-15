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
package com.impetus.client.onetomany;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * The Class OTMNPerson.
 */
@Entity
@Table(name = "PERSON", schema = "test")
public class OTMNPerson
{

    /** The person id. */
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String personName;

    // @OneToMany(cascade=CascadeType.ALL)
    /** The addresses. */
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "person")
    // @JoinColumn(name="PERSON_ID")
    private Set<OTMAddress> addresses;

    /**
     * Gets the person id.
     * 
     * @return the person id
     */
    public String getPersonId()
    {
        return personId;
    }

    /**
     * Gets the person name.
     * 
     * @return the person name
     */
    public String getPersonName()
    {
        return personName;
    }

    /**
     * Sets the person name.
     * 
     * @param personName
     *            the new person name
     */
    public void setPersonName(String personName)
    {
        this.personName = personName;
    }

    /**
     * Sets the person id.
     * 
     * @param personId
     *            the new person id
     */
    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    /**
     * Gets the addresses.
     * 
     * @return the addresses
     */
    public Set<OTMAddress> getAddresses()
    {
        return addresses;
    }

    /**
     * Sets the addresses.
     * 
     * @param addresses
     *            the new addresses
     */
    public void setAddresses(Set<OTMAddress> addresses)
    {
        this.addresses = addresses;
    }

}
