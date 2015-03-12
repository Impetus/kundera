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
package com.impetus.client.hbase.compositetype;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * The Class HBaseCompoundKey.
 * 
 * @author vivek.mishra
 */
@Embeddable
public class HBaseCompoundKey
{

    /** The user id. */
    @Column
    private String userId;

    /** The tweet id. */
    @Column
    private int tweetId;

    /** The time line id. */
    @Column
    private UUID timeLineId;

    /**
     * Instantiates a new HBase compound key.
     */
    public HBaseCompoundKey()
    {
    }

    /**
     * Instantiates a new HBase compound key.
     * 
     * @param userId
     *            the user id
     * @param tweetId
     *            the tweet id
     * @param timeLineId
     *            the time line id
     */
    public HBaseCompoundKey(String userId, int tweetId, UUID timeLineId)
    {
        this.userId = userId;
        this.tweetId = tweetId;
        this.timeLineId = timeLineId;
    }

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
     * Gets the tweet id.
     * 
     * @return the tweetId
     */
    public int getTweetId()
    {
        return tweetId;
    }

    /**
     * Gets the time line id.
     * 
     * @return the timeLineId
     */
    public UUID getTimeLineId()
    {
        return timeLineId;
    }

}
