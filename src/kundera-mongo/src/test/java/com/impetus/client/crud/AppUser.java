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
package com.impetus.client.crud;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "User", schema = "KunderaExamples@mongoTest")
public class AppUser
{
    @Id
    private String id;

    @Column
    private List<String> tags;

    @Column
    private Map<String, String> propertyKeys;

    @Column
    private Set<String> nickNames;

    @Column
    protected List<String> friendList;

    @Embedded
    private PhoneDirectory phoneDirectory;

    public AppUser()
    {
        tags = new LinkedList<String>();
        propertyKeys = new HashMap<String, String>();
        nickNames = new HashSet<String>();
        friendList = new LinkedList<String>();
        tags.add("yo");
        propertyKeys.put("kk", "Kuldeep");
        nickNames.add("kk");
        friendList.add("xamry");
        friendList.add("mevivs");
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public List<String> getTags()
    {
        return tags;
    }

    public Map<String, String> getPropertyKeys()
    {
        return propertyKeys;
    }

    public Set<String> getNickName()
    {
        return nickNames;
    }

    public List<String> getFriendList()
    {
        return friendList;
    }

    public PhoneDirectory getPhoneDirectory()
    {
        return phoneDirectory;
    }

    public void setPropertyContainer(PhoneDirectory propertyContainer)
    {
        this.phoneDirectory = propertyContainer;
    }
}
