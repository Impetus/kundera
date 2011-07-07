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
package com.impetus.kundera.entity;

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
 * Entity Class for User object
 *
 * @author amresh.singh
 */

@Entity
@Table(name = "users", schema = "Blog")
public class User
{
    @Id
    private String userId; // PK

    // Embedded object, will persist co-located
    @Embedded
    private PersonalDetail personalDetail;

    // Embedded collection, will persist co-located
    @Embedded
    private List<Tweet> tweets;

    // One-to-one, will be persisted separately
    @OneToOne(cascade = { CascadeType.ALL }, fetch = FetchType.LAZY)
    private Preference preference;

    // One to many, will be persisted separately
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.LAZY)
    private Set<IMDetail> imDetails;

    /**
     * @return the userId
     */
    public String getUserId()
    {
        return userId;
    }

    /**
     * @param userId
     *            the userId to set
     */
    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    /**
     * @return the personalDetail
     */
    public PersonalDetail getPersonalDetail()
    {
        return personalDetail;
    }

    /**
     * @param personalDetail
     *            the personalDetail to set
     */
    public void setPersonalDetail(PersonalDetail personalDetail)
    {
        this.personalDetail = personalDetail;
    }

    /**
     * @return the tweets
     */
    public List<Tweet> getTweets()
    {
        return tweets;
    }

    /**
     * @param tweets
     *            the tweets to set
     */
    public void addTweet(Tweet tweet)
    {
        if (this.tweets == null || this.tweets.isEmpty())
        {
            this.tweets = new ArrayList<Tweet>();
        }
        this.tweets.add(tweet);
    }

    /**
     * @return the preference
     */
    public Preference getPreference()
    {
        return preference;
    }

    /**
     * @param preference
     *            the preference to set
     */
    public void setPreference(Preference preference)
    {
        this.preference = preference;
    }

    /**
     * @return the imDetails
     */
    public Set<IMDetail> getImDetails()
    {
        return imDetails;
    }

    /**
     * @param imDetails
     *            the imDetails to set
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
