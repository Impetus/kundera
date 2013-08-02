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
import javax.persistence.Embeddable;

@Embeddable
public class PhoneDirectory
{
    @Column
    private String phoneDirectoryName;

    @Column
    private List<String> contactName;

    @Column
    private Map<String, String> contactMap;

    @Column
    private Set<String> contactNumber;

    public PhoneDirectory()
    {
        contactName = new LinkedList<String>();
        contactMap = new HashMap<String, String>();
        contactNumber = new HashSet<String>();
        contactName.add("xamry");
        contactMap.put("xamry", "9891991919");
        contactNumber.add("9891991919");
        phoneDirectoryName = "MyPhoneDirectory";
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
