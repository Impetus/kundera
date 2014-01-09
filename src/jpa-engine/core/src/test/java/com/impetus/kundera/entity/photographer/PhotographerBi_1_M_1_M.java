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
package com.impetus.kundera.entity.photographer;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.impetus.kundera.entity.PersonalDetail;
import com.impetus.kundera.entity.Tweet;
import com.impetus.kundera.entity.album.AlbumBi_1_M_1_M;

/**
 * Entity class representing a photographer
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "PHOTOGRAPHER", schema = "KunderaTest@kunderatest")
public class PhotographerBi_1_M_1_M
{
    @Id
    @Column(name = "PHOTOGRAPHER_ID")
    private int photographerId;

    @Column(name = "PHOTOGRAPHER_NAME")
    private String photographerName;

    // Embedded object, will persist co-located
    @Embedded
    private PersonalDetail personalDetail;

    // Element collection, will persist co-located
    @ElementCollection
    @CollectionTable(name = "tweeted")
    private List<Tweet> tweets;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "photographer")
    private List<AlbumBi_1_M_1_M> albums;

    /**
     * @return the photographerId
     */
    public int getPhotographerId()
    {
        return photographerId;
    }

    /**
     * @param photographerId
     *            the photographerId to set
     */
    public void setPhotographerId(int photographerId)
    {
        this.photographerId = photographerId;
    }

    /**
     * @return the photographerName
     */
    public String getPhotographerName()
    {
        return photographerName;
    }

    /**
     * @param photographerName
     *            the photographerName to set
     */
    public void setPhotographerName(String photographerName)
    {
        this.photographerName = photographerName;
    }

    /**
     * @return the albums
     */
    public List<AlbumBi_1_M_1_M> getAlbums()
    {
        return albums;
    }

    /**
     * @param albums
     *            the albums to set
     */
    public void addAlbum(AlbumBi_1_M_1_M album)
    {
        if (this.albums == null || this.albums.isEmpty())
        {
            this.albums = new ArrayList<AlbumBi_1_M_1_M>();
        }
        this.albums.add(album);
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
    public void setTweets(List<Tweet> tweets)
    {
        this.tweets = tweets;
    }

    /**
     * @param tweets
     *            the tweets to set
     */
    public void addTweet(Tweet tweet)
    {
        if (tweets == null)
        {
            tweets = new ArrayList<Tweet>();
        }
        tweets.add(tweet);
    }

    /**
     * @param albums
     *            the albums to set
     */
    public void setAlbums(List<AlbumBi_1_M_1_M> albums)
    {
        this.albums = albums;
    }

}
