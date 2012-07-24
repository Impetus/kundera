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
package com.impetus.client;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

/**
 * The Class Player.
 */
@Entity
@Table(name = "player", schema = "test")
public class Player implements Serializable
{

    /** The id. */
    @Id
    // @GeneratedValue
    private String id;

    /** The last name. */
    @Column(name = "lname", nullable = false)
    private String lastName;

    /** The first name. */
    @Column(name = "fname", nullable = false)
    private String firstName;

    /** The jersey number. */
    @Column(name = "jnumber", nullable = false)
    private int jerseyNumber;

    /** The last spoken words. */
    @Column(name = "lword", nullable = true)
    private String lastSpokenWords;

    /**
     * Creates a new instance of Player.
     */
    public Player()
    {
    }

    /**
     * Gets the id of this Player. The persistence provider should autogenerate
     * a unique id for new player objects.
     * 
     * @return the id
     */

    public String getId()
    {
        return this.id;
    }

    /**
     * Sets the id of this Player to the specified value.
     * 
     * @param id
     *            the new id
     */
    public void setId(String id)
    {
        this.id = id;
    }

    /**
     * Gets the last name.
     * 
     * @return the last name
     */
    public String getLastName()
    {
        return lastName;
    }

    /**
     * Sets the last name.
     * 
     * @param name
     *            the new last name
     */
    public void setLastName(String name)
    {
        lastName = name;
    }

    // ...
    // some code excluded for brevity
    // ...

    /**
     * Returns the last words spoken by this player. We don't want to persist
     * that!
     * 
     * @return the last spoken words
     */
    @Transient
    public String getLastSpokenWords()
    {
        return lastSpokenWords;
    }

    /**
     * Sets the last spoken words.
     * 
     * @param lastWords
     *            the new last spoken words
     */
    public void setLastSpokenWords(String lastWords)
    {
        lastSpokenWords = lastWords;
    }

    /**
     * Gets the first name.
     * 
     * @return the first name
     */
    public String getFirstName()
    {
        return firstName;
    }

    /**
     * Sets the first name.
     * 
     * @param firstName
     *            the new first name
     */
    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    /**
     * Gets the jersey number.
     * 
     * @return the jersey number
     */
    public int getJerseyNumber()
    {
        return jerseyNumber;
    }

    /**
     * Sets the jersey number.
     * 
     * @param jerseyNumber
     *            the new jersey number
     */
    public void setJerseyNumber(int jerseyNumber)
    {
        this.jerseyNumber = jerseyNumber;
    }

}
