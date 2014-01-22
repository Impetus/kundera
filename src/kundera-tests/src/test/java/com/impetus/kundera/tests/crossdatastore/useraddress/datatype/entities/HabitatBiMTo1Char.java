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
package com.impetus.kundera.tests.crossdatastore.useraddress.datatype.entities;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@IndexCollection(columns = { @Index(name = "street") })
@Table(name = "ADDRESS_INT", schema = "KunderaTests@addCassandra")
public class HabitatBiMTo1Char
{
    @Id
    @Column(name = "ADDRESS_ID")
    private char addressId;

    @Column(name = "STREET")
    private String street;

    @OneToMany(mappedBy = "address", fetch = FetchType.LAZY)
    private Set<PersonnelBiMTo1Int> people;

    public char getAddressId()
    {
        return addressId;
    }

    public void setAddressId(char addressId)
    {
        this.addressId = addressId;
    }

    public String getStreet()
    {
        return street;
    }

    public void setStreet(String street)
    {
        this.street = street;
    }

    public Set<PersonnelBiMTo1Int> getPeople()
    {
        return people;
    }

    public void setPeople(Set<PersonnelBiMTo1Int> people)
    {
        this.people = people;
    }

}
