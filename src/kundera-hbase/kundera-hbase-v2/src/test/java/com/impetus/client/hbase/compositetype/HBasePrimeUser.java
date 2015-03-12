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
package com.impetus.client.hbase.compositetype;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * The Class HBasePrimeUser.
 * 
 * @author vivek.mishra
 */

@Entity
@Table(name = "PRIME_USER", schema = "HBaseNew@queryTest")
public class HBasePrimeUser
{

    /** The key. */
    @EmbeddedId
    private HBaseCompoundKey key;

    /** The tweet body. */
    @Column
    private String tweetBody;

    /** The tweet date. */
    @Column
    private Date tweetDate;

    /**
     * Instantiates a new HBase prime user.
     */
    public HBasePrimeUser()
    {
    }

    /**
     * Instantiates a new HBase prime user.
     * 
     * @param key
     *            the key
     */
    public HBasePrimeUser(HBaseCompoundKey key)
    {
        this.key = key;
    }

    /**
     * Gets the key.
     * 
     * @return the key
     */
    public HBaseCompoundKey getKey()
    {
        return key;
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
}
