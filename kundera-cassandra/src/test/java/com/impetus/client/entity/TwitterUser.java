/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.entity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

/**
 * Entity Class for User object.
 *
 * @author amresh.singh
 */

@Entity
@Table(name = "users", schema = "Blog")
public class TwitterUser
{

    /** The user id. */
    @Id
    private String userId; // PK

    // Embedded object, will persist co-located
    /** The personal detail. */
    @Embedded
    private PersonalDetail personalDetail;

    // Embedded collection, will persist co-located
    /** The tweets. */
    @Embedded
    private List<UserTweet> tweets;

    // One-to-one, will be persisted separately
    /** The preference. */
    @OneToOne(cascade = { CascadeType.ALL }, fetch = FetchType.LAZY)
    private UserPreference preference;

    // One to many, will be persisted separately
    /** The im details. */
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.LAZY)
    private Set<IMDetail> imDetails;

    /**
     * Gets the user id.
     *
     * @return the userId
     */
    public String getUserId()
    {
        return userId;
    }

    /**
     * Sets the user id.
     *
     * @param userId the userId to set
     */
    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    /**
     * Gets the personal detail.
     *
     * @return the personalDetail
     */
    public PersonalDetail getPersonalDetail()
    {
        return personalDetail;
    }

    /**
     * Sets the personal detail.
     *
     * @param personalDetail the personalDetail to set
     */
    public void setPersonalDetail(PersonalDetail personalDetail)
    {
        this.personalDetail = personalDetail;
    }

    /**
     * Gets the tweets.
     *
     * @return the tweets
     */
    public List<UserTweet> getTweets()
    {
        return tweets;
    }

    /**
     * Adds the tweet.
     *
     * @param tweet the tweet
     */
    public void addTweet(UserTweet tweet)
    {
        if (this.tweets == null || this.tweets.isEmpty())
        {
            this.tweets = new ArrayList<UserTweet>();
        }
        this.tweets.add(tweet);
    }

    /**
     * Gets the preference.
     *
     * @return the preference
     */
    public UserPreference getPreference()
    {
        return preference;
    }

    /**
     * Sets the preference.
     *
     * @param preference the preference to set
     */
    public void setPreference(UserPreference preference)
    {
        this.preference = preference;
    }

    /**
     * Gets the im details.
     *
     * @return the imDetails
     */
    public Set<IMDetail> getImDetails()
    {
        return imDetails;
    }

    /**
     * Adds the im detail.
     *
     * @param imDetail the im detail
     */
    public void addImDetail(IMDetail imDetail)
    {
        if (this.imDetails == null || this.imDetails.isEmpty())
        {
            this.imDetails = new HashSet<IMDetail>();
        }

        this.imDetails.add(imDetail);
    }

}
