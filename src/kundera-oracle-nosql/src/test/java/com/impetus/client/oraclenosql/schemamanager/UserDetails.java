/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.oraclenosql.schemamanager;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * The Class UserDetails.
 * 
 * @author devender.yadav
 */
@Embeddable
@IndexCollection(columns = { @Index(indexName = "email", name = "email"),
        @Index(indexName = "address", name = "address") })
public class UserDetails
{

    /** The email. */
    @Column(name = "email")
    private String email;

    /** The address. */
    @Column(name = "address")
    private String address;

    /**
     * Gets the email.
     * 
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }

    /**
     * Sets the email.
     * 
     * @param email
     *            the new email
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    /**
     * Gets the address.
     * 
     * @return the address
     */
    public String getAddress()
    {
        return address;
    }

    /**
     * Sets the address.
     * 
     * @param address
     *            the new address
     */
    public void setAddress(String address)
    {
        this.address = address;
    }

}
