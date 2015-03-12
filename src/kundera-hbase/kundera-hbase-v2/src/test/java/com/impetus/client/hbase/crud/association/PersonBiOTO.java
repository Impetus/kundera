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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

/**
 * The Class PersonBiOTO.
 * 
 * @author Devender Yadav
 */
@Entity
@Table(name = "PERSON_BOTO", schema = "HBaseNew@associationTest")
public class PersonBiOTO
{

    /** The person id. */
    @Id
    @Column(name = "PERSON_ID")
    private int personId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String personName;

    /** The age. */
    @Column(name = "AGE")
    private int age;

    /**
     * Instantiates a new person bi oto.
     */
    public PersonBiOTO()
    {

    }

    /**
     * Instantiates a new person bi oto.
     * 
     * @param personId
     *            the person id
     * @param personName
     *            the person name
     * @param age
     *            the age
     */
    public PersonBiOTO(int personId, String personName, int age)
    {
        this.personId = personId;
        this.personName = personName;
        this.age = age;
    }

    /** The address. */
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn(name = "ADDRESS_ID")
    private AddressBiOTO address;

    /**
     * Gets the person id.
     * 
     * @return the person id
     */
    public int getPersonId()
    {
        return personId;
    }

    /**
     * Sets the person id.
     * 
     * @param personId
     *            the new person id
     */
    public void setPersonId(int personId)
    {
        this.personId = personId;
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
     * Gets the age.
     * 
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     * 
     * @param age
     *            the new age
     */
    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * Gets the address.
     * 
     * @return the address
     */
    public AddressBiOTO getAddress()
    {
        return address;
    }

    /**
     * Sets the address.
     * 
     * @param address
     *            the new address
     */
    public void setAddress(AddressBiOTO address)
    {
        this.address = address;
    }

}
