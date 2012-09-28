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
package com.impetus.client.twitter.entities;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.client.twitter.utils.ExampleUtils;
import com.impetus.kundera.annotations.Index;

/**
 * Entity class for user's personal details
 * 
 * @author amresh.singh
 */

@Embeddable
@Index(index=true, columns={"name", "relationshipStatus"})
public class PersonalDetail
{
    @Column(name = "personal_detail_id")
    private String personalDetailId;

    @Column(name = "person_name")
    private String name;

    @Column(name = "person_password")
    private String password;

    @Column(name = "rel_status")
    private String relationshipStatus;

    public PersonalDetail()
    {

    }

    public PersonalDetail(String name, String password, String relationshipStatus)
    {
        setPersonalDetailId(ExampleUtils.getUniqueId());
        setName(name);
        setPassword(password);
        setRelationshipStatus(relationshipStatus);
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

}
