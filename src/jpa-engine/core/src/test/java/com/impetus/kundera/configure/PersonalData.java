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
package com.impetus.kundera.configure;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * The Class CorePersonalData.
 */
@Embeddable
@IndexCollection(columns = { @Index(name = "email"), @Index(name = "yahooId")})
public class PersonalData
{

    /** The website. */
    @Column(name = "department_website")
    private String website;

    /** The email. */
    @Column(name = "department_email")
    private String email;

    /** The yahoo id. */
    @Column(name = "department_yahoo_id")
    private String yahooId;

    /**
     * Instantiates a new core personal data.
     */
    public PersonalData()
    {

    }

    /**
     * Instantiates a new core personal data.
     * 
     * @param website
     *            the website
     * @param email
     *            the email
     * @param yahooId
     *            the yahoo id
     */
    public PersonalData(String website, String email, String yahooId)
    {
        this.website = website;
        this.email = email;
        this.yahooId = yahooId;
    }

    /**
     * Gets the website.
     * 
     * @return the website
     */
    public String getWebsite()
    {
        return website;
    }

    /**
     * Sets the website.
     * 
     * @param website
     *            the new website
     */
    public void setWebsite(String website)
    {
        this.website = website;
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
     *            the email to set
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    /**
     * Gets the yahoo id.
     * 
     * @return the yahoo id
     */
    public String getYahooId()
    {
        return yahooId;
    }

    /**
     * Sets the yahoo id.
     * 
     * @param yahooId
     *            the new yahoo id
     */
    public void setYahooId(String yahooId)
    {
        this.yahooId = yahooId;
    }
}
