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
package com.impetus.hbase.entity;

import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.entity.PersonalDetail;

/**
 * Entity class for User
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "huser", schema = "Blog")
public class HUser
{
    @Id
    private String userId;
    
    //Embedded object, will persist co-located
    @Embedded
    private PersonalDetail personalDetail;

    /**
     * @return the userId
     */
    public String getUserId()
    {
        return userId;
    }

    /**
     * @param userId the userId to set
     */
    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    /**
     * @return the personalDetail
     */
    public PersonalDetail getPersonalDetail()
    {
        return personalDetail;
    }

    /**
     * @param personalDetail the personalDetail to set
     */
    public void setPersonalDetail(PersonalDetail personalDetail)
    {
        this.personalDetail = personalDetail;
    }       

}
