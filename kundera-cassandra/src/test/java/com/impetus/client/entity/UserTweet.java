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

import javax.persistence.Embeddable;


/**
 * Class for Tweets.
 *
 * @author amresh.singh
 */

@Embeddable
public class UserTweet
{

    /** The tweet id. */
    private String tweetId;

    /** The body. */
    private String body;

    /** The device. */
    private String device;

    /**
     * Instantiates a new user tweet.
     *
     * @param tweetId the tweet id
     * @param body the body
     * @param device the device
     */
    public UserTweet(String tweetId, String body, String device)
    {
        this.tweetId = tweetId;
        this.body = body;
        this.device = device;
    }

    /**
     * Instantiates a new user tweet.
     */
    public UserTweet()
    {

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
     * @param tweetId the tweetId to set
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
     * @param body the body to set
     */
    public void setBody(String body)
    {
        this.body = body;
    }

    /**
     * Gets the device.
     *
     * @return the device
     */
    public String getDevice()
    {
        return device;
    }

    /**
     * Sets the device.
     *
     * @param device the device to set
     */
    public void setDevice(String device)
    {
        this.device = device;
    }

}
