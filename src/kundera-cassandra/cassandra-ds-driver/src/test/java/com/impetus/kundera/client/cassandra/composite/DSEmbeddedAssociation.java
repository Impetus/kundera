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
package com.impetus.kundera.client.cassandra.composite;

import java.util.Date;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.impetus.client.cassandra.crud.compositeType.association.UserInfo;

/**
 * Composite key entity with association columns
 * 
 * @author vivek.mishra
 * 
 */

@Entity
@Table(name = "CompositeUserAssociation", schema = "CompositeCassandra@composite_pu")
// @Index(index = true,columns = { "tweetBody","tweetDate" })
public class DSEmbeddedAssociation
{

    @EmbeddedId
    private UserTimeLine key;

    @Column
    private String tweetBody;

    @Column
    private Date tweetDate;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "userInfo_id")
    private UserInfo userInfo;

    public DSEmbeddedAssociation()
    {
    }

    public DSEmbeddedAssociation(UserTimeLine key)
    {
        this.key = key;
    }

    /**
     * @return the key
     */
    public UserTimeLine getKey()
    {
        return key;
    }

    /**
     * @return the tweetBody
     */
    public String getTweetBody()
    {
        return tweetBody;
    }

    /**
     * @return the tweetDate
     */
    public Date getTweetDate()
    {
        return tweetDate;
    }

    /**
     * @param tweetBody
     *            the tweetBody to set
     */
    public void setTweetBody(String tweetBody)
    {
        this.tweetBody = tweetBody;
    }

    /**
     * @param tweetDate
     *            the tweetDate to set
     */
    public void setTweetDate(Date tweetDate)
    {
        this.tweetDate = tweetDate;
    }

    public UserInfo getUserInfo()
    {
        return userInfo;
    }

    public void setUserInfo(UserInfo userInfo)
    {
        this.userInfo = userInfo;
    }

}
