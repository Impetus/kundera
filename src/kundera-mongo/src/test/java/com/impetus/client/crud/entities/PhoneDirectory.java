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
package com.impetus.client.crud.entities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.validation.constraints.Size;

@Embeddable
public class PhoneDirectory
{
    @Column
    private String phoneDirectoryName;

    @Column
    private List<String> contactName;

    @Column
    @Size(message = "The size should be at least equal to one but not more than 2", min = 1, max = 2)
    private Map<String, String> contactMap;

    @Column
    private Set<String> contactNumber;

    public PhoneDirectory()
    {
        this.contactName = new LinkedList<String>();
        this.contactMap = new HashMap<String, String>();
    }

    public PhoneDirectory(String phoneDirectoryName, List<String> contactName, Map<String, String> contactMap,
            Set<String> contactNumber)
    {
        this.contactName = contactName;
        this.contactMap = contactMap;
        this.contactNumber = contactNumber;
        this.phoneDirectoryName = phoneDirectoryName;
    }

    public String getPhoneDirectoryName()
    {
        return phoneDirectoryName;
    }

    public List<String> getContactName()
    {
        return contactName;
    }

    public Map<String, String> getContactMap()
    {
        return contactMap;
    }

    public Set<String> getContactNumber()
    {
        return contactNumber;
    }
}
