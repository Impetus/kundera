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

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * The Class CoreEntityPersonUni1ToM.
 */
@Entity
@Table(name = "CoreEntityPersonUni1ToM", schema = "KunderaCoreExmples@cassandra")
public class CoreEntityPersonUni1ToM
{

    /** The person id. */
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String personName;

    /** The age. */
    @Column(name = "AGE")
    private short age;

    /** The personal data. */
    @Embedded
    private PersonalData personalData;

    /** The addresses. */
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "PERSON_ID")
    private Set<CoreEntityAddressUni1ToM> addresses;

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
     * Gets the age.
     * 
     * @return the age
     */
    public short getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     * 
     * @param age
     *            the age to set
     */
    public void setAge(short age)
    {
        this.age = age;
    }

    /**
     * Gets the personal data.
     * 
     * @return the personalData
     */
    public PersonalData getPersonalData()
    {
        return personalData;
    }

    /**
     * Sets the personal data.
     * 
     * @param personalData
     *            the personalData to set
     */
    public void setPersonalData(PersonalData personalData)
    {
        this.personalData = personalData;
    }

    /**
     * Gets the addresses.
     * 
     * @return the addresses
     */
    public Set<CoreEntityAddressUni1ToM> getAddresses()
    {
        return addresses;
    }

    /**
     * Sets the addresses.
     * 
     * @param addresses
     *            the addresses to set
     */
    public void setAddresses(Set<CoreEntityAddressUni1ToM> addresses)
    {
        this.addresses = addresses;
    }

}
