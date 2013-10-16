/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.es.association;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 *  Bi M-M address entity.
 */
@Entity
//@Table(name = "ADDRESSBIMM", schema="esSchema@es-pu")
public class AddressBiMM
{
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    @Column(name = "STREET")
    private String street;

    @ManyToMany(mappedBy = "addresses", fetch = FetchType.EAGER)
    private Set<PersonBiMM> people;


    public String getAddressId()
    {
        return addressId;
    }

    public void setAddressId(String addressId)
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

    public Set<PersonBiMM> getPeople()
    {
        return people;
    }

    public void setPeople(Set<PersonBiMM> people)
    {
        this.people = people;
    }

    
}
