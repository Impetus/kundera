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
package com.impetus.kundera.client.cassandra.pelops.crud;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * Facebook profile entity
 * @author vivek.mishra
 *
 */
@Entity
@DiscriminatorValue("fb")
public class FacebookProfile extends SocialProfile
{
    // protected static final String TYPE = "twitter";

    // @Id
    // // @GeneratedValue
    // @Column(name = "guid", updatable = false, nullable = false)
    // private String id;

    @Column(name = "facebook_id", updatable = false)
    private String facebookId;

    @Column(name = "facebook_user", length = 128)
    private String facebookUser;

    public String getFacebookId()
    {
        return facebookId;
    }

    public void setFacebookId(String facebookId)
    {
        this.facebookId = facebookId;
    }

    public String getFacebookUser()
    {
        return facebookUser;
    }

    public void setFacebookUser(String facebookUser)
    {
        this.facebookUser = facebookUser;
    }

}
