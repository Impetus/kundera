/*
 * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.tests.embeddedRDBMS;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.junit.experimental.categories.Category;

/**
 * Entity class for user's personal details
 * 
 * @author amresh.singh
 */

@Embeddable
public class PersonalDetail
{
    @Column(name = "personal_detail_id")
    private String personalDetailId;

    @Column(name = "name")
    private String name;

    @Column(name = "password")
    private String password;

    @Column(name = "rel_status")
    private String relationshipStatus;

    @Column(name = "age")
    private Short age;

    public PersonalDetail()
    {

    }

    public PersonalDetail(String name, String password, String relationshipStatus, Short age)
    {
        setPersonalDetailId(UUID.randomUUID().toString());
        setName(name);
        setPassword(password);
        setRelationshipStatus(relationshipStatus);
        setAge(age);
    }

    /**
     * @return the personalDetailId
     */
    public String getPersonalDetailId()
    {
        return personalDetailId;
    }

    /**
     * @param personalDetailId
     *            the personalDetailId to set
     */
    public void setPersonalDetailId(String personalDetailId)
    {
        this.personalDetailId = personalDetailId;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * @return the password
     */
    public String getPassword()
    {
        return password;
    }

    /**
     * @param password
     *            the password to set
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * @return the relationshipStatus
     */
    public String getRelationshipStatus()
    {
        return relationshipStatus;
    }

    /**
     * @param relationshipStatus
     *            the relationshipStatus to set
     */
    public void setRelationshipStatus(String relationshipStatus)
    {
        this.relationshipStatus = relationshipStatus;
    }

    public Short getAge()
    {
        return age;
    }

    public void setAge(Short age)
    {
        this.age = age;
    }

}
