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

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

// TODO: Auto-generated Javadoc
/**
 * The Class PersonUDT.
 */
@Entity
@Table(name = "person_udt", schema = "UdtTest@cassandra_udt")
@IndexCollection(columns = { @Index(name = "nicknames"), @Index(name = "email"), @Index(name = "password") })
public class PersonUDT
{

    /** The person id. */
    @Id
    @Column
    private String personId;

    /** The personal details. */
    @Embedded
    private PersonalDetailsUDT personalDetails;

    /** The professional details. */
    @Embedded
    private ProfessionalDetailsUDT professionalDetails;

    /** The email. */
    @Column
    private String email;

    /** The password. */
    @Column
    private String password;

    /** The nicknames. */
    @Column
    private List<String> nicknames;

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
     * Sets the person id.
     *
     * @param personId the new person id
     */
    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    /**
     * Gets the personal details.
     *
     * @return the personal details
     */
    public PersonalDetailsUDT getPersonalDetails()
    {
        return personalDetails;
    }

    /**
     * Sets the personal details.
     *
     * @param personalDetails the new personal details
     */
    public void setPersonalDetails(PersonalDetailsUDT personalDetails)
    {
        this.personalDetails = personalDetails;
    }

    /**
     * Gets the professional details.
     *
     * @return the professional details
     */
    public ProfessionalDetailsUDT getProfessionalDetails()
    {
        return professionalDetails;
    }

    /**
     * Sets the professional details.
     *
     * @param professionalDetails the new professional details
     */
    public void setProfessionalDetails(ProfessionalDetailsUDT professionalDetails)
    {
        this.professionalDetails = professionalDetails;
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
     * @param email the new email
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    /**
     * Gets the password.
     *
     * @return the password
     */
    public String getPassword()
    {
        return password;
    }

    /**
     * Sets the password.
     *
     * @param password the new password
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Gets the nicknames.
     *
     * @return the nicknames
     */
    public List<String> getNicknames()
    {
        return nicknames;
    }

    /**
     * Sets the nicknames.
     *
     * @param nicknames the new nicknames
     */
    public void setNicknames(List<String> nicknames)
    {
        this.nicknames = nicknames;
    }

}
