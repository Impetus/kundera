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
package com.impetus.kundera.rest.common;


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
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author impetus
 * 
 */
@Entity
@Table(name = "USER")
@XmlRootElement
//@XmlAccessorType(XmlAccessType.FIELD)
@IndexCollection(columns = { @Index(name = "preference") })
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
    @OneToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "PREFERENCE_ID")
    //@XmlTransient
    private PreferenceCassandra preference;

    // One to many, will be persisted separately
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "USER_ID")
    private Set<ExternalLink> externalLinks;
    
    @Column(name = "USER_IMAGE")
    private byte[] userImage;

   

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
    public void setTweets(List<TweetCassandra> tweets)
    {
        this.tweets = tweets;
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
    public Set<ExternalLink> getExternalLinks()
    {
        return externalLinks;
    }

    /**
     * @param imDetails
     *            the imDetails to set
     */
    public void setExternalLinks(Set<ExternalLink> externalLinks)
    {
        this.externalLinks = externalLinks;
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
    public void setFriends(List<UserCassandra> friends)
    {
        this.friends = friends;
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
    public void setFollowers(List<UserCassandra> followers)
    {
        this.followers = followers ;
    }
    
    /**
   	 * @return the userImage
   	 */
   	public byte[] getUserImage() {
   		return userImage;
   	}

   	/**
   	 * @param userImage the userImage to set
   	 */
   	public void setUserImage(byte[] userImage) {
   		this.userImage = userImage;
   	}

}
