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
package com.impetus.kundera.client.crud.inheritence;


import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToMany;
import javax.persistence.Table;


/**
 * User account entity.
 * @author vivek.mishra
 *
 */
@Entity
@Table(name = "user_account")
public class UserAccount extends GuidDomainObject
{
   
    @Column(name = "display_name", length = 128)
    private String displayName = null;

    @Column(name = "email", length = 128, unique = true)
    private String email = null;

    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER,mappedBy = "account")
    private List<SocialProfile> profiles;
    
    public String getDispName()
    {
        return displayName;
    }

    public void setDispName(String displayName)
    {
        this.displayName = displayName;
    }
    
    public String getEmail()
    {
        return email;
    }

    public void setEmail(String email)
    {
        this.email = email;
    }
    
    public List<SocialProfile> getSocialProfiles()
    {
        return profiles;
    }

    public void setSocialProfiles(List<SocialProfile> profiles)
    {
        this.profiles = profiles;
    }
} 
