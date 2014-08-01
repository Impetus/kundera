/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author vivek.mishra
 */
@Embeddable
@XmlRootElement
public class MongoCompoundKey
{
    @Column
    private String userId;

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Column
    private int tweetId;

    public void setTweetId(int tweetId) {
        this.tweetId = tweetId;
    }

    @Column
    private UUID timeLineId;

    public void setTimeLineId(UUID timeLineId) {
        this.timeLineId = timeLineId;
    }

    /**
     * 
     */
    public MongoCompoundKey()
    {
    }

    /**
     * @param userId
     * @param tweetId
     * @param timeLineId
     */
    public MongoCompoundKey(String userId, int tweetId, UUID timeLineId)
    {
        this.userId = userId;
        this.tweetId = tweetId;
        this.timeLineId = timeLineId;
    }

    /**
     * @return the userId
     */
    public String getUserId()
    {
        return userId;
    }

    /**
     * @return the tweetId
     */
    public int getTweetId()
    {
        return tweetId;
    }

    /**
     * @return the timeLineId
     */
    public UUID getTimeLineId()
    {
        return timeLineId;
    }

}
