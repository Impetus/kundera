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

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * The Class Fullname.
 */
@Embeddable
public class Fullname
{
    
    /** The first name. */
    @Column
    private String firstname;

    /** The middle name. */
    @Column
    private String middlename;

    /** The last name. */
    @Column
    private String lastname;

    /**
     * Instantiates a new fullname.
     */
    public Fullname()
    {

    }

    /**
     * Gets the first name.
     *
     * @return the first name
     */
    public String getFirstName()
    {
        return firstname;
    }

    /**
     * Sets the first name.
     *
     * @param firstName the new first name
     */
    public void setFirstName(String firstName)
    {
        this.firstname = firstName;
    }

    /**
     * Gets the middle name.
     *
     * @return the middle name
     */
    public String getMiddleName()
    {
        return middlename;
    }

    /**
     * Sets the middle name.
     *
     * @param middleName the new middle name
     */
    public void setMiddleName(String middleName)
    {
        this.middlename = middleName;
    }

    /**
     * Gets the last name.
     *
     * @return the last name
     */
    public String getLastName()
    {
        return lastname;
    }

    /**
     * Sets the last name.
     *
     * @param lastName the new last name
     */
    public void setLastName(String lastName)
    {
        this.lastname = lastName;
    }

}
