/*******************************************************************************
 * * Copyright 2017 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.client.crud.entities;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * The Class CompositeId.
 */
@Embeddable
public class CompositeId
{

    /** The first name. */
    @Basic
    @Column(name = "first_name")
    private String firstName;

    /** The birth date. */
    @Basic
    @Column(name = "birth_date")
    private String birthDate;

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
    public void setFirstName(final String firstName)
    {
        this.firstName = firstName;
    }

    /**
     * Gets the birth date.
     * 
     * @return the birth date
     */
    public String getBirthDate()
    {
        return birthDate;
    }

    /**
     * Sets the birth date.
     * 
     * @param birthDate
     *            the new birth date
     */
    public void setBirthDate(final String birthDate)
    {
        this.birthDate = birthDate;
    }
}
