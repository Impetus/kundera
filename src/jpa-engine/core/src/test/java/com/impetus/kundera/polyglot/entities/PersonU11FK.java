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
package com.impetus.kundera.polyglot.entities;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "PERSON", schema = "KunderaTests@patest")
@IndexCollection(columns = { @Index(name = "personName") })
public class PersonU11FK
{
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "ADDRESS_ID")
    private AddressU11FK address;

    public String getPersonId()
    {
        return personId;
    }

    public String getPersonName()
    {
        return personName;
    }

    public void setPersonName(String personName)
    {
        this.personName = personName;
    }

    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    public AddressU11FK getAddress()
    {
        return address;
    }

    public void setAddress(AddressU11FK address)
    {
        this.address = address;
    }

}
