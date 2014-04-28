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

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 * 
 *         Composite user entity
 */
@Entity
@Table(name = "TwitterUser", schema = "KunderaCoreExmples@CassandraSchemaOperationTest")
public class TwitterUser
{

    @EmbeddedId
    private CompositeUser compositeKey;

    @Column
    private String body;

    @ElementCollection
    private Set<String> followers;

    public TwitterUser()
    {

    }

    public CompositeUser getCompositeKey()
    {
        return compositeKey;
    }

    public void setCompositeKey(CompositeUser compositeKey)
    {
        this.compositeKey = compositeKey;
    }

    public String getBody()
    {
        return body;
    }

    public void setBody(String body)
    {
        this.body = body;
    }

    public Set<String> getFollowers()
    {
        return followers;
    }

    public void setFollowers(Set<String> followers)
    {
        this.followers = followers;
    }

}
