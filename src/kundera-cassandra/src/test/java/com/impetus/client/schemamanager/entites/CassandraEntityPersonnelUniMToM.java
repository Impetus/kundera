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
package com.impetus.client.schemamanager.entites;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

@Entity
@Table(name = "CassandraEntityPersonnelUniMToM", schema = "KunderaCassandraMTMExamples@cassandra")
public class CassandraEntityPersonnelUniMToM
{
    @Id
    @Column(name = "PERSON_ID")
    private String personId;

    @Column(name = "PERSON_NAME")
    private String personName;

    @Embedded
    CassandraPersonalData personalData;

    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinTable(name = "PERSONNEL_ADDRESS", joinColumns = { @JoinColumn(name = "PERSON_ID") }, inverseJoinColumns = { @JoinColumn(name = "ADDRESS_ID") })
    private Set<CassandraEntityHabitatUniMToM> addresses;

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

    public Set<CassandraEntityHabitatUniMToM> getAddresses()
    {
        return addresses;
    }

    public void setAddresses(Set<CassandraEntityHabitatUniMToM> addresses)
    {
        this.addresses = addresses;
    }

    /**
     * @return the personalData
     */
    public CassandraPersonalData getPersonalData()
    {
        return personalData;
    }

    /**
     * @param personalData
     *            the personalData to set
     */
    public void setPersonalData(CassandraPersonalData personalData)
    {
        this.personalData = personalData;
    }
}
