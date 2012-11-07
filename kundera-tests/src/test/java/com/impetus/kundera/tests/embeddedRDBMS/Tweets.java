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
package com.impetus.kundera.tests.embeddedRDBMS;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.impetus.kundera.annotations.Index;

/**
 * @author vivek.mishra
 * 
 */
/**
 * create column family tweets with comparator=UTF8Type and
 * column_metadata=[{column_name: body, validation_class: UTF8Type, index_type:
 * KEYS}, {column_name: tweeted_at, validation_class: DateType, index_type:
 * KEYS}, {column_name: user_id, validation_class: UTF8Type, index_type: KEYS}];
 */
@Entity
@Table(name = "tweets", schema = "KunderaTests@secIdxAddCassandra")
@Index(columns = { "body", "tweeted_at" }, index = true)
public class Tweets
{

    @Id
    @Column(name = "tweet_id")
    private String tweetId;

    @Column(name = "body")
    private String body;

    @Column(name = "tweeted_at")
    @Temporal(TemporalType.DATE)
    private Date tweetDate;

    public Tweets()
    {
        // Default constructor.
    }

    /**
     * @return the tweetId
     */
    public String getTweetId()
    {
        return tweetId;
    }

    /**
     * @param tweetId
     *            the tweetId to set
     */
    public void setTweetId(String tweetId)
    {
        this.tweetId = tweetId;
    }

    /**
     * @return the body
     */
    public String getBody()
    {
        return body;
    }

    /**
     * @param body
     *            the body to set
     */
    public void setBody(String body)
    {
        this.body = body;
    }

    /**
     * @return the tweetDate
     */
    public Date getTweetDate()
    {
        return tweetDate;
    }

    /**
     * @param tweetDate
     *            the tweetDate to set
     */
    public void setTweetDate(Date tweetDate)
    {
        this.tweetDate = tweetDate;
    }

}
