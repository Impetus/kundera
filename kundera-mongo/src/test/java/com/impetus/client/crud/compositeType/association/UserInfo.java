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
package com.impetus.client.crud.compositeType.association;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.impetus.client.crud.compositeType.MongoPrimeUser;

/**
 * @author vivek.mishra
 * 
 */

@Entity
@Table(name = "UserInfo", schema = "KunderaExamples@mongoTest")
public class UserInfo
{

    @Id
    @Column(name = "userInfo_id")
    private String userInfoId;

    @Column(name = "first_name")
    private String firstName;

    @Column(name = "last_name")
    private String lastName;

    @Column(name = "age")
    private int age;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "user_id")
    private MongoPrimeUser timeLine;

    /**
     * 
     */
    public UserInfo()
    {
    }

    /**
     * @param userInfoId
     * @param firstName
     * @param lastName
     * @param age
     * @param timeLine
     */
    public UserInfo(String userInfoId, String firstName, String lastName, int age, MongoPrimeUser timeLine)
    {
        super();
        this.userInfoId = userInfoId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.timeLine = timeLine;
    }

    /**
     * @return the userInfoId
     */
    public String getUserInfoId()
    {
        return userInfoId;
    }

    /**
     * @return the firstName
     */
    public String getFirstName()
    {
        return firstName;
    }

    /**
     * @return the lastName
     */
    public String getLastName()
    {
        return lastName;
    }

    /**
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * @return the timeLine
     */
    public MongoPrimeUser getTimeLine()
    {
        return timeLine;
    }

    /**
     * @param firstName
     *            the firstName to set
     */
    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(int age)
    {
        this.age = age;
    }
}
