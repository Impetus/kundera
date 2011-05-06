/*
 * Copyright 2011 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import com.impetus.kundera.api.Document;

/**
 * The Contact Entity Class
 * @author amresh.singh
 */

@Entity
@Document(name="contact", db="mongodbtest")
public class Contact {
	@Id
	@Column(name="contact_id")
	String contactId;
	
	@Column(name="last_name")
	String lastName;
	
	@Column(name="first_name")
	String firstName;
	
	@Column(name="email_id")
	String emailId;
	
	public Contact() {
		
	}
	
	public Contact(String contactId, String firstName, String lastName, String emailId) {
		this.contactId = contactId;
		this.firstName = firstName;
		this.lastName = lastName;
		this.emailId = emailId;
	}
	
	public String toString() {
		return firstName + " " + lastName + " <" + emailId + ">";
	}
	
	/**
	 * @return the contactId
	 */
	public String getContactId() {
		return contactId;
	}
	/**
	 * @param contactId the contactId to set
	 */
	public void setContactId(String contactId) {
		this.contactId = contactId;
	}
	/**
	 * @return the lastName
	 */
	public String getLastName() {
		return lastName;
	}
	/**
	 * @param lastName the lastName to set
	 */
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	/**
	 * @return the firstName
	 */
	public String getFirstName() {
		return firstName;
	}
	/**
	 * @param firstName the firstName to set
	 */
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	/**
	 * @return the emailId
	 */
	public String getEmailId() {
		return emailId;
	}
	/**
	 * @param emailId the emailId to set
	 */
	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}

}
