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
package com.impetus.client.crud.gfs;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

/**
 * The Class GFSUser.
 * 
 * @author Devender Yadav
 */
@Entity
@Table(name = "USER", schema = "GFS@gfs_pu")
public class GFSUser
{

    /** The user id. */
    @Id
    @Column(name = "USER_ID")
    private int userId;

    /** The name. */
    @Column(name = "NAME")
    private String name;

    /** The profile pic. */
    @Lob
    @Column(name = "PROFILE_PIC")
    private byte[] profilePic;

    /**
     * Gets the user id.
     * 
     * @return the user id
     */
    public int getUserId()
    {
        return userId;
    }

    /**
     * Sets the user id.
     * 
     * @param userId
     *            the new user id
     */
    public void setUserId(int userId)
    {
        this.userId = userId;
    }

    /**
     * Gets the name.
     * 
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     * 
     * @param name
     *            the new name
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Gets the profile pic.
     * 
     * @return the profile pic
     */
    public byte[] getProfilePic()
    {
        return profilePic;
    }

    /**
     * Sets the profile pic.
     * 
     * @param profilePic
     *            the new profile pic
     */
    public void setProfilePic(byte[] profilePic)
    {
        this.profilePic = profilePic;
    }
}
