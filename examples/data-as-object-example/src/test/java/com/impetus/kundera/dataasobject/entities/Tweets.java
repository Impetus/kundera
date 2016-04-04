/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.kundera.dataasobject.entities;

import java.util.Date;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.impetus.core.DefaultKunderaEntity;

/**
 * The Class Tweets.
 *
 * @author impetus
 * 
 *         Tweets entity
 */

@Entity
public class Tweets extends DefaultKunderaEntity<Tweets, String>
{

    /** The tweet id. */
    @Id
    @Column(name = "tweet_id")
    private String tweetId;

    /** The body. */
    @Column(name = "body")
    private String body;

    /** The tweet date. */
    @Column(name = "tweeted_at")
    @Temporal(TemporalType.DATE)
    private Date tweetDate;

    /** The videos. */
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "tweet_id")
    private Set<Video> videos;

    /**
     * Instantiates a new tweets.
     */
    public Tweets()
    {
        // Default constructor.
    }

    /**
     * Gets the tweet id.
     *
     * @return the tweetId
     */
    public String getTweetId()
    {
        return tweetId;
    }

    /**
     * Sets the tweet id.
     *
     * @param tweetId
     *            the tweetId to set
     */
    public void setTweetId(String tweetId)
    {
        this.tweetId = tweetId;
    }

    /**
     * Gets the body.
     *
     * @return the body
     */
    public String getBody()
    {
        return body;
    }

    /**
     * Sets the body.
     *
     * @param body
     *            the body to set
     */
    public void setBody(String body)
    {
        this.body = body;
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
     * Gets the videos.
     *
     * @return the videos
     */
    public Set<Video> getVideos()
    {
        return videos;
    }

    /**
     * Sets the videos.
     *
     * @param videos
     *            the new videos
     */
    public void setVideos(Set<Video> videos)
    {
        this.videos = videos;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "Tweets [tweetId=" + tweetId + ", body=" + body + ", tweetDate=" + tweetDate + ", videos=" + videos
                + "]";
    }

}
