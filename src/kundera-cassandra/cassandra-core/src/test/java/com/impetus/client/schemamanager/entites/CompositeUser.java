/*******************************************************************************
 *  * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.schemamanager.entites;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Embeddable
//@IndexCollection(columns = { @Index(name = "tweetDate"), @Index(name = "firstName") })
public class CompositeUser
{

    @Column
    private String user_id;

    @Column
    private Date tweetDate;

    @Column
    private String firstName;

    public CompositeUser()
    {
        
    }
    
    public String getUser_id()
    {
        return user_id;
    }

    public void setUser_id(String user_id)
    {
        this.user_id = user_id;
    }

    public Date getTweetDate()
    {
        return tweetDate;
    }

    public void setTweetDate(Date tweetDate)
    {
        this.tweetDate = tweetDate;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

}
