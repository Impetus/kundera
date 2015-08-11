/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.crud.association;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * The Class UserOTM.
 * 
 * @author devender.yadav
 */
@Entity
@Table(name = "USER_OTM", schema = "HBaseNew@associationTest")
public class UserOTM
{

    /** The user id. */
    @Id
    @Column(name = "USER_ID")
    private String userId;

    /** The username. */
    @Column(name = "USER_NAME")
    private String username;

    /** The followers. */
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.LAZY)
    @JoinColumn(name = "FOLLOWER_ID")
    private List<UserOTM> followers;

    /**
     * Adds the follower.
     * 
     * @param follower
     *            the follower
     */
    public void addFollower(UserOTM follower)
    {
        if (this.followers == null || this.followers.isEmpty())
        {
            this.followers = new ArrayList<UserOTM>();
        }

        this.followers.add(follower);
    }

    /**
     * Sets the user id.
     * 
     * @param userId
     *            the new user id
     */
    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    /**
     * Gets the user id.
     * 
     * @return the user id
     */
    public String getUserId()
    {
        return userId;
    }

    /**
     * Gets the username.
     * 
     * @return the username
     */
    public String getUsername()
    {
        return username;
    }

    /**
     * Sets the username.
     * 
     * @param username
     *            the new username
     */
    public void setUsername(String username)
    {
        this.username = username;
    }

    /**
     * Gets the followers.
     * 
     * @return the followers
     */
    public List<UserOTM> getFollowers()
    {
        return followers;
    }

    /**
     * Sets the followers.
     * 
     * @param followers
     *            the new followers
     */
    public void setFollowers(List<UserOTM> followers)
    {
        this.followers = followers;
    }
}
