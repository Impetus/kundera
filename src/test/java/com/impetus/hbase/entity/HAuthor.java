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
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * The Class Author.
 * 
 * @author animesh.kumar
 */
@Entity
// makes it an entity class
@Table(name = "hAuthor", schema = "Blog")
// assign ColumnFamily type and name
public class HAuthor implements Serializable
{

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The username. */
    @Id
    // row identifier
    private String username;

    /** The email address. */
    @Column(name = "email")
    // override column-name
    private String emailAddress;

    /** The PRIME. */
    private static final int PRIME = 31;

    /** The country. */
    @Column
    private String country;

    /** The registered. */
    @Column(name = "registeredSince")
    @Temporal(TemporalType.DATE)
    @Basic
    private Date registered;

    /**
     * Instantiates a new author.
     */
    public HAuthor()
    { // must have a default constructor
    }

    // getters, setters etc.

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        int result = 1;
        result = PRIME * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj)
    {
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        if (obj == null)
        {
            ret = false;
        }
        if (!(obj instanceof HAuthor))
        {
            ret = false;
        }
        final HAuthor other = (HAuthor) obj;
        if (username == null)
        {
            if (other.username != null)
            {
                ret = false;
            }
        }
        else if (!username.equals(other.username))
        {
            ret = false;
        }
        return ret;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        final StringBuilder builder = new StringBuilder();
        builder.append("Author [country=");
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
    public String getUsername()
    {
        return username;
    }

    /**
     * Sets the username.
     * 
     * @param username
     *            the username to set
     */
    public void setUsername(final String username)
    {
        this.username = username;
    }

    /**
     * Gets the email address.
     * 
     * @return the emailAddress
     */
    public String getEmailAddress()
    {
        return emailAddress;
    }

    /**
     * Sets the email address.
     * 
     * @param emailAddress
     *            the emailAddress to set
     */
    public void setEmailAddress(final String emailAddress)
    {
        this.emailAddress = emailAddress;
    }

    /**
     * Gets the country.
     * 
     * @return the country
     */
    public String getCountry()
    {
        return country;
    }

    /**
     * Sets the country.
     * 
     * @param country
     *            the country to set
     */
    public void setCountry(final String country)
    {
        this.country = country;
    }

    /**
     * Gets the registered.
     * 
     * @return the registered
     */
    public Date getRegistered()
    {
        return registered;
    }

    /**
     * Sets the registered.
     * 
     * @param registered
     *            the registered to set
     */
    public void setRegistered(final Date registered)
    {
        this.registered = registered;
    }

}
