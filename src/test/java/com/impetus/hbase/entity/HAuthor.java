/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.hbase.entity;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.impetus.kundera.api.ColumnFamily;

/**
 * The Class Author.
 * 
 * @author animesh.kumar
 */
@Entity
// makes it an entity class
@ColumnFamily(family="hAuthor", keyspace="Blog")
// assign ColumnFamily type and name
public class HAuthor implements Serializable{

    /** The username. */
    @Id
    // row identifier
    String username;

    /** The email address. */
    @Column(name = "email")
    // override column-name
    String emailAddress;

    /** The country. */
    @Column
    String country;

    /** The registered. */
    @Column(name = "registeredSince")
    @Temporal(TemporalType.DATE)
    @Basic
    Date registered;

    /** The name. */
    String name;

   
    /**
     * Instantiates a new author.
     */
    public HAuthor() { // must have a default constructor
    }

    // getters, setters etc.

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof HAuthor))
            return false;
        HAuthor other = (HAuthor) obj;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Author [name=");
		builder.append(name);
		builder.append(", country=");
		builder.append(country);
		builder.append(", emailAddress=");
		builder.append(emailAddress);
		builder.append(", registered=");
		builder.append(registered);
		builder.append(", username=");
		builder.append(username);
		builder.append("]");
		return builder.toString();
	}

	/**
	 * Gets the username.
	 * 
	 * @return the username
	 */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the username.
     * 
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets the email address.
     * 
     * @return the emailAddress
     */
    public String getEmailAddress() {
        return emailAddress;
    }

    /**
     * Sets the email address.
     * 
     * @param emailAddress the emailAddress to set
     */
    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    /**
     * Gets the country.
     * 
     * @return the country
     */
    public String getCountry() {
        return country;
    }

    /**
     * Sets the country.
     * 
     * @param country the country to set
     */
    public void setCountry(String country) {
        this.country = country;
    }

    /**
     * Gets the registered.
     * 
     * @return the registered
     */
    public Date getRegistered() {
        return registered;
    }

    /**
     * Sets the registered.
     * 
     * @param registered the registered to set
     */
    public void setRegistered(Date registered) {
        this.registered = registered;
    }
  
    
}
