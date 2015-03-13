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
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class PersonEmbed.
 * 
 * @author Pragalbh Garg
 */
@Entity
@Table(name = "PERSON", schema = "HBaseNew@embeddablesTest")
public class PersonEmbed
{

    /** The person id. */
    @Id
    @Column(name = "ID")
    private int personId;

    /** The personal details. */
    @Embedded
    private PersonalDetails personal;

    /** The professional details. */
    @Embedded
    private ProfessionalDetails professional;

    /** The email. */
    @Column(name = "EMAIL")
    private String email;

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
     * Gets the personal details.
     * 
     * @return the personal details
     */
    public PersonalDetails getPersonalDetails()
    {
        return personal;
    }

    /**
     * Sets the personal details.
     * 
     * @param personalDetails
     *            the new personal details
     */
    public void setPersonalDetails(PersonalDetails personalDetails)
    {
        this.personal = personalDetails;
    }

    /**
     * Gets the professional details.
     * 
     * @return the professional details
     */
    public ProfessionalDetails getProfessionalDetails()
    {
        return professional;
    }

    /**
     * Sets the professional details.
     * 
     * @param professionalDetails
     *            the new professional details
     */
    public void setProfessionalDetails(ProfessionalDetails professionalDetails)
    {
        this.professional = professionalDetails;
    }

    /**
     * Gets the email.
     * 
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }

    /**
     * Sets the email.
     * 
     * @param email
     *            the new email
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

}
