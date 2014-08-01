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
package com.impetus.kundera.rest.common;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonProperty;

@Entity
@Table(name = "PERSONNEL")
@XmlRootElement
public class PersonnelUni1ToM
{
    @Id
    @Column(name = "PERSON_ID")
    private String PERSON_ID;

    @Column(name = "PERSON_NAME")
    private String personName;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "PERSON_ID")
    private Set<HabitatUni1ToM> HabitatUni1ToM;

    @JsonProperty("PERSON_ID")
    public String getPersonId()
    {
        return PERSON_ID;
    }

    public String getPersonName()
    {
        return personName;
    }

    public void setPersonName(String personName)
    {
        this.personName = personName;
    }
    @JsonProperty("PERSON_ID")
    public void setPersonId(String personId)
    {
        this.PERSON_ID = personId;
    }
    @JsonProperty("HabitatUni1ToM")
    public Set<HabitatUni1ToM> getAddresses()
    {
        return HabitatUni1ToM;
    }
    @JsonProperty("HabitatUni1ToM")
    public void setAddresses(Set<HabitatUni1ToM> addresses)
    {
        this.HabitatUni1ToM = addresses;
    }
}
