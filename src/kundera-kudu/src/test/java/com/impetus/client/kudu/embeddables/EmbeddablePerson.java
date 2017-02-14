/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu.embeddables;

import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class Person.
 * 
 * @author: karthikp.manchala
 */
@Entity
@Table(name = "PERSON_EMBED_KUDU", schema = "kudutest@kudu")
public class EmbeddablePerson
{

    /** The person id. */
    @Id
    private String personId;

    /** The person name. */
    private String personName;
    
    @Embedded
    private Address address;

    public Address getAddress()
    {
        return address;
    }

    public void setAddress(Address address)
    {
        this.address = address;
    }

    /**
     * Instantiates a new person.
     */
    public EmbeddablePerson()
    {
    }

    /**
     * Instantiates a new person.
     *
     * @param personId
     *            the person id
     * @param personName
     *            the person name
     * @param age
     *            the age
     * @param salary
     *            the salary
     */
    public EmbeddablePerson(String personId, String personName, Address address)
    {
        super();
        this.personId = personId;
        this.personName = personName;
        this.address = address;
    }

    /**
     * Gets the person id.
     * 
     * @return the person id
     */
    public String getPersonId()
    {
        return personId;
    }

    /**
     * Sets the person id.
     * 
     * @param personId
     *            the new person id
     */
    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    /**
     * Gets the person name.
     * 
     * @return the person name
     */
    public String getPersonName()
    {
        return personName;
    }

    /**
     * Sets the person name.
     * 
     * @param personName
     *            the new person name
     */
    public void setPersonName(String personName)
    {
        this.personName = personName;
    }


}
