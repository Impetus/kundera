/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.tests.file.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Entity Class for User's profile picture
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "PROFILE_PICTURE", schema = "KunderaTests@secIdxAddCassandra")
public class ProfilePicture
{
    @Id
    private int profilePicId;

    @Column(name = "FULL_PICTURE")
    private byte[] fullPicture;

    @Column(name = "CROPPED_PICTURE")
    private byte[] croppedPicture;

    @Column(name = "SMALL_PICTURE")
    private byte[] smallPicture;

    /**
     * @return the profilePicId
     */
    public int getProfilePicId()
    {
        return profilePicId;
    }

    /**
     * @param profilePicId
     *            the profilePicId to set
     */
    public void setProfilePicId(int profilePicId)
    {
        this.profilePicId = profilePicId;
    }

    /**
     * @return the fullPicture
     */
    public byte[] getFullPicture()
    {
        return fullPicture;
    }

    /**
     * @param fullPicture
     *            the fullPicture to set
     */
    public void setFullPicture(byte[] fullPicture)
    {
        this.fullPicture = fullPicture;
    }

    /**
     * @return the croppedPicture
     */
    public byte[] getCroppedPicture()
    {
        return croppedPicture;
    }

    /**
     * @param croppedPicture
     *            the croppedPicture to set
     */
    public void setCroppedPicture(byte[] croppedPicture)
    {
        this.croppedPicture = croppedPicture;
    }

    /**
     * @return the smallPicture
     */
    public byte[] getSmallPicture()
    {
        return smallPicture;
    }

    /**
     * @param smallPicture
     *            the smallPicture to set
     */
    public void setSmallPicture(byte[] smallPicture)
    {
        this.smallPicture = smallPicture;
    }

}
