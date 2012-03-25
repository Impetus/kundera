/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Contact Entity Class.
 *
 * @author amresh.singh
 */

@Entity
@Table(name = "contact", schema = "Blog")
public class Contact
{

    /** The contact id. */
    @Id
    String contactId;

    /** The last name. */
    @Column(name = "last_name")
    String lastName;

    /** The first name. */
    @Column(name = "first_name")
    String firstName;

    /** The email id. */
    @Column(name = "email_id")
    String emailId;

    /**
     * Instantiates a new contact.
     */
    public Contact()
    {

    }

    /**
     * Instantiates a new contact.
     *
     * @param contactId the contact id
     * @param firstName the first name
     * @param lastName the last name
     * @param emailId the email id
     */
    public Contact(String contactId, String firstName, String lastName, String emailId)
    {
        this.contactId = contactId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.emailId = emailId;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return firstName + " " + lastName + " <" + emailId + ">";
    }

    /**
     * Gets the contact id.
     *
     * @return the contactId
     */
    public String getContactId()
    {
        return contactId;
    }

    /**
     * Sets the contact id.
     *
     * @param contactId the contactId to set
     */
    public void setContactId(String contactId)
    {
        this.contactId = contactId;
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
     * @param lastName the lastName to set
     */
    public void setLastName(String lastName)
    {
        this.lastName = lastName;
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
     * @param firstName the firstName to set
     */
    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    /**
     * Gets the email id.
     *
     * @return the emailId
     */
    public String getEmailId()
    {
        return emailId;
    }

    /**
     * Sets the email id.
     *
     * @param emailId the emailId to set
     */
    public void setEmailId(String emailId)
    {
        this.emailId = emailId;
    }

}
