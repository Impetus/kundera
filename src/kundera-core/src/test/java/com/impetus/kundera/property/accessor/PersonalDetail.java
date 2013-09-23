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
package com.impetus.kundera.property.accessor;

import java.io.Serializable;
import java.util.UUID;

import javax.persistence.Column;

/**
 * Class used as an object for conversion within test cases 
 * @author amresh.singh
 */

public class PersonalDetail implements Serializable
{
    @Column(name = "personal_detail_id")
    private String personalDetailId;

    @Column(name = "name")
    private String name;

    @Column(name = "password")
    private String password;

    @Column(name = "rel_status")
    private String relationshipStatus;

    public PersonalDetail()
    {

    }

    public PersonalDetail(String name, String password, String relationshipStatus)
    {
        setPersonalDetailId(UUID.randomUUID().toString());
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
    
    @Override
    public boolean equals(Object o)
    {
        if (this == o ) return true;       
        if (!(o instanceof PersonalDetail) ) return false;
        
        PersonalDetail that = (PersonalDetail)o;
        
        return that.getPersonalDetailId().equals(this.getPersonalDetailId())
        && that.getName().equals(this.getName())
        && that.getPassword().equals(this.getPassword())
        && that.getRelationshipStatus().equals(this.getRelationshipStatus());
    }

}
