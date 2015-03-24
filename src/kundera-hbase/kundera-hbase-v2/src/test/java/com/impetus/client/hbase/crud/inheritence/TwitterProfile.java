/**
 * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.crud.inheritence;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * Twitter profile entity.
 * 
 * @author Pragalbh Garg
 * 
 */
@Entity
@DiscriminatorValue("twitter")
public class TwitterProfile extends SocialProfile
{
    /** The twitter id. */
    @Column(name = "twitter_id", updatable = false)
    private String twitterId;

    /** The twitter user. */
    @Column(name = "twitter_user", length = 128)
    private String twitterUser;

    /**
     * Gets the twitter id.
     * 
     * @return the twitter id
     */
    public String getTwitterId()
    {
        return twitterId;
    }

    /**
     * Sets the twitter id.
     * 
     * @param twitterId
     *            the new twitter id
     */
    public void setTwitterId(String twitterId)
    {
        this.twitterId = twitterId;
    }

    /**
     * Gets the twitter name.
     * 
     * @return the twitter name
     */
    public String getTwitterName()
    {
        return twitterUser;
    }

    /**
     * Sets the twitter name.
     * 
     * @param twitterUser
     *            the new twitter name
     */
    public void setTwitterName(String twitterUser)
    {
        this.twitterUser = twitterUser;
    }

}
