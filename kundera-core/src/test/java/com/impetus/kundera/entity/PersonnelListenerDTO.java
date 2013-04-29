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
package com.impetus.kundera.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class PersonnelDTO.
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "person", schema = "kunderatest@kunderatest")
@EntityListeners({ PersonHandler.class })
public class PersonnelListenerDTO
{

    /** The person id. */
    @Id
    private String personId;

    /** The first name. */
    @Column(name = "first_name")
    private String firstName;

    /** The last name. */
    @Column(name = "last_name")
    private String lastName;

    /**
     * Instantiates a new personnel dto.
     * 
     * @param personId
     *            the person id
     * @param firstName
     *            the first name
     * @param lastName
     *            the last name
     */
    public PersonnelListenerDTO(String personId, String firstName, String lastName)
    {
        this.personId = personId;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * Instantiates a new personnel dto.
     */
    public PersonnelListenerDTO()
    {

    }

    /**
     * Gets the person id.
     * 
     * @return the personId
     */
    public String getPersonId()
    {
        return personId;
    }

    /**
     * Sets the person id.
     * 
     * @param personId
     *            the personId to set
     */
    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    /**
     * Gets the first name.
     * 
     * @return the firstName
     */
    public String getFirstName()
    {
        return firstName;
    }

    /**
     * Sets the first name.
     * 
     * @param firstName
     *            the firstName to set
     */
    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    /**
     * Gets the last name.
     * 
     * @return the lastName
     */
    public String getLastName()
    {
        return lastName;
    }

    /**
     * Sets the last name.
     * 
     * @param lastName
     *            the lastName to set
     */
    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }

}
