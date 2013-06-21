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
package com.impetus.client.twitter.entities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

/**
 * @author impetus
 * 
 */
@Entity
@Table(name = "USER", schema = "KunderaExamples@twissandraTest")
public class UserCassandra
{

    @Id
    @Column(name = "USER_ID")
    private String userId;

    // Embedded object, will persist co-located
    @Embedded
    private PersonalDetailCassandra personalDetail;

    // Embedded object, will persist co-located
    @Embedded
    private ProfessionalDetailCassandra professionalDetail;

    // Element collection, will persist co-located
    @ElementCollection
    @CollectionTable(name = "tweeted")
    private List<TweetCassandra> tweets;

    // One to many, will be persisted separately
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FRIEND_ID")
    private List<UserCassandra> friends; // List of users whom I follow

    // One to many, will be persisted separately
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FOLLOWER_ID")
    private List<UserCassandra> followers; // List of users who are following me

    // One-to-one, will be persisted separately
    @OneToOne(cascade = { CascadeType.ALL }, fetch = FetchType.LAZY)
    @JoinColumn(name = "PREFERENCE_ID")
    private PreferenceCassandra preference;

    // One to many, will be persisted separately
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.LAZY)
    @JoinColumn(name = "USER_ID")
    private Set<ExternalLinkCassandra> externalLinks;

    public UserCassandra()
    {

    }

    public UserCassandra(String userId, String name, String password, String relationshipStatus)
    {
        PersonalDetailCassandra pd = new PersonalDetailCassandra(name, password, relationshipStatus);
        setUserId(userId);
        setPersonalDetail(pd);
    }

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
    public PersonalDetailCassandra getPersonalDetail()
    {
        return personalDetail;
    }

    /**
     * @param personalDetail
     *            the personalDetail to set
     */
    public void setPersonalDetail(PersonalDetailCassandra personalDetail)
    {
        this.personalDetail = personalDetail;
    }

    /**
     * @return the professionalDetail
     */
    public ProfessionalDetailCassandra getProfessionalDetail()
    {
        return professionalDetail;
    }

    /**
     * @param professionalDetail
     *            the professionalDetail to set
     */
    public void setProfessionalDetail(ProfessionalDetailCassandra professionalDetail)
    {
        this.professionalDetail = professionalDetail;
    }

    /**
     * @return the tweets
     */
    public List<TweetCassandra> getTweets()
    {
        return tweets;
    }

    /**
     * @param tweets
     *            the tweets to set
     */
    public void addTweet(TweetCassandra tweet)
    {
        if (this.tweets == null || this.tweets.isEmpty())
        {
            this.tweets = new ArrayList<TweetCassandra>();
        }
        this.tweets.add(tweet);
    }

    /**
     * @return the preference
     */
    public PreferenceCassandra getPreference()
    {
        return preference;
    }

    /**
     * @param preference
     *            the preference to set
     */
    public void setPreference(PreferenceCassandra preference)
    {
        this.preference = preference;
    }

    /**
     * @return the externalLinks
     */
    public Set<ExternalLinkCassandra> getExternalLinks()
    {
        return externalLinks;
    }

    /**
     * @param imDetails
     *            the imDetails to set
     */
    public void addExternalLink(ExternalLinkCassandra externalLink)
    {
        if (this.externalLinks == null || this.externalLinks.isEmpty())
        {
            this.externalLinks = new HashSet<ExternalLinkCassandra>();
        }

        this.externalLinks.add(externalLink);
    }

   /**
     * @return the friends
     */
    public List<UserCassandra> getFriends()
    {
        return friends;
    }

    /**
     * @param friends
     *            the friends to set
     */
    public void addFriend(UserCassandra friend)
    {
        if (this.friends == null || this.friends.isEmpty())
        {
            this.friends = new ArrayList<UserCassandra>();
        }
        this.friends.add(friend);
    }

    /**
     * @return the followers
     */
    public List<UserCassandra> getFollowers()
    {
        return followers;
    }

    /**
     * @param followers
     *            the followers to set
     */
    public void addFollower(UserCassandra follower)
    {
        if (this.followers == null || this.followers.isEmpty())
        {
            this.followers = new ArrayList<UserCassandra>();
        }

        this.followers.add(follower);
    }

}
