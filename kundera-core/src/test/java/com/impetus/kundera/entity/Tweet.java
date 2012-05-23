/*
 * Copyright 2011 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.entity;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * Class for Tweets
 * 
 * @author amresh.singh
 */

@Embeddable
public class Tweet
{

    @Column(name = "tweet_id")
    private String tweetId;

    @Column(name = "tweet_body")
    private String body;

    @Column(name = "tweeted_from")
    private String device;


    public Tweet(String body, String device)
    {
        this.tweetId = UUID.randomUUID().toString();
        this.body = body;
        this.device = device;
    }

    public Tweet()
    {

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
     * @return the device
     */
    public String getDevice()
    {
        return device;
    }

    /**
     * @param device
     *            the device to set
     */
    public void setDevice(String device)
    {
        this.device = device;
    }

    /*    *//**
     * @return the timestamp
     */
    /*
     * public long getTimestamp() { return timestamp; }
     *//**
     * @param timestamp
     *            the timestamp to set
     */
    /*
     * public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
     */
}
