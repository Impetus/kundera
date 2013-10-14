/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql.entities;

import java.io.File;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * UserProfile entity class
 * 
 * @author amresh.singh
 */
@Entity
@Table(name = "USER_PROFILE", schema = "OracleNoSqlTests@twikvstore")
@IndexCollection(columns = { @Index(name = "userName") })
public class UserProfile
{
    @Id
    @Column(name = "USER_ID")
    private int userId;

    @Column(name = "USER_NAME")
    private String userName;

    @Column(name = "PROFILE_PICTURE")
    private File profilePicture;

    public UserProfile()
    {

    }

    public UserProfile(int userId, String userName, File profilePicture)
    {
        super();
        this.userId = userId;
        this.userName = userName;
        this.profilePicture = profilePicture;
    }

    /**
     * @return the userId
     */
    public int getUserId()
    {
        return userId;
    }

    /**
     * @param userId
     *            the userId to set
     */
    public void setUserId(int userId)
    {
        this.userId = userId;
    }

    /**
     * @return the userName
     */
    public String getUserName()
    {
        return userName;
    }

    /**
     * @param userName
     *            the userName to set
     */
    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    /**
     * @return the profilePicture
     */
    public File getProfilePicture()
    {
        return profilePicture;
    }

    /**
     * @param profilePicture
     *            the profilePicture to set
     */
    public void setProfilePicture(File profilePicture)
    {
        this.profilePicture = profilePicture;
    }

}
