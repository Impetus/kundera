/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kvapps.entities;

import java.util.Date;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author impetus
 * 
 *         Tweets entity
 */

@Entity
@Table(name = "tweets")
@IndexCollection(columns = { @Index(name = "body"), @Index(name = "tweeted_at") })
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

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "tweet_id")
    private Set<Video> videos;

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

    public Set<Video> getVideos()
    {
        return videos;
    }

    public void setVideos(Set<Video> videos)
    {
        this.videos = videos;
    }

    @Override
    public String toString()
    {
        return "Tweets [tweetId=" + tweetId + ", body=" + body + ", tweetDate=" + tweetDate + ", videos=" + videos
                + "]";
    }

}
