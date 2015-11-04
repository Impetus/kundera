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
package com.impetus.client.oraclenosql.entities;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class OracleNoSqlEmbeddedUser.
 * 
 * @author vivek.mishra
 */

@Entity
@Table(name = "Embedded_User")
public class OracleNoSqlEmbeddedUser
{

    /** The user id. */
    @Id
    private String userId;

    /** The embeddable. */
    @Embedded
    private OracleNoSqlCompoundKey embeddable;

    /** The tweet body. */
    @Column
    private String tweetBody;

    /** The tweet date. */
    @Column
    private Date tweetDate;

    /**
     * Instantiates a new redis embedded user.
     */
    public OracleNoSqlEmbeddedUser()
    {
    }

    /**
     * Instantiates a new redis embedded user.
     * 
     * @param userId
     *            the user id
     */
    public OracleNoSqlEmbeddedUser(String userId)
    {
        this.userId = userId;
    }

    /**
     * Gets the tweet body.
     * 
     * @return the tweetBody
     */
    public String getTweetBody()
    {
        return tweetBody;
    }

    /**
     * Gets the tweet date.
     * 
     * @return the tweetDate
     */
    public Date getTweetDate()
    {
        return tweetDate;
    }

    /**
     * Sets the tweet body.
     * 
     * @param tweetBody
     *            the tweetBody to set
     */
    public void setTweetBody(String tweetBody)
    {
        this.tweetBody = tweetBody;
    }

    /**
     * Sets the tweet date.
     * 
     * @param tweetDate
     *            the tweetDate to set
     */
    public void setTweetDate(Date tweetDate)
    {
        this.tweetDate = tweetDate;
    }

    /**
     * Gets the user id.
     * 
     * @return the user id
     */
    public String getUserId()
    {
        return userId;
    }

    /**
     * Gets the embeddable.
     * 
     * @return the embeddable
     */
    public OracleNoSqlCompoundKey getEmbeddable()
    {
        return embeddable;
    }

    /**
     * Sets the embeddable.
     * 
     * @param embeddable
     *            the new embeddable
     */
    public void setEmbeddable(OracleNoSqlCompoundKey embeddable)
    {
        this.embeddable = embeddable;
    }

}
