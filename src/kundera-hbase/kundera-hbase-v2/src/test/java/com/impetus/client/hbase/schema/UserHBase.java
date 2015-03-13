/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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

package com.impetus.client.hbase.schema;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class UserHBase.
 * 
 * @author Pragalbh Garg
 */
@Entity
@Table(name = "USER_HBASE", schema = "HBaseNew@schemaTest")
public class UserHBase
{

    /** The person id. */
    @Id
    @Column(name = "PERSON_ID")
    private String userId;

    /** The person name. */
    @Column(name = "PERSON_NAME")
    private String userName;

    /** The age. */
    @Column(name = "EMAIL_ID")
    private String email;

    /** The phone no. */
    @Column(name = "PHONE_NO")
    private long phoneNo;

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
     * Gets the user name.
     * 
     * @return the user name
     */
    public String getUserName()
    {
        return userName;
    }

    /**
     * Sets the user name.
     * 
     * @param userName
     *            the new user name
     */
    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    /**
     * Gets the email.
     * 
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }

    /**
     * Sets the email.
     * 
     * @param email
     *            the new email
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    /**
     * Gets the phone no.
     * 
     * @return the phone no
     */
    public long getPhoneNo()
    {
        return phoneNo;
    }

    /**
     * Sets the phone no.
     * 
     * @param phoneNo
     *            the new phone no
     */
    public void setPhoneNo(long phoneNo)
    {
        this.phoneNo = phoneNo;
    }

}
