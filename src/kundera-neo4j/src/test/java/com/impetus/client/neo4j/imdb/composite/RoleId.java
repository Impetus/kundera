/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.neo4j.imdb.composite;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * Class Holding Identity attributes for roles
 * 
 * @author amresh.singh
 */
@Embeddable
public class RoleId
{
    @Column(name = "FIRST_NAME")
    private String firstName;

    @Column(name = "LAST_NAME")
    private String lastName;

    public RoleId()
    {
    }

    /**
     * @param firstName
     * @param lastName
     */
    public RoleId(String firstName, String lastName)
    {
        super();
        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * @return the firstName
     */
    public String getFirstName()
    {
        return firstName;
    }

    /**
     * @param firstName
     *            the firstName to set
     */
    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    /**
     * @return the lastName
     */
    public String getLastName()
    {
        return lastName;
    }

    /**
     * @param lastName
     *            the lastName to set
     */
    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }

    public boolean equals(Object o)
    {
        if (!(o instanceof RoleId))
        {
            return false;
        }

        RoleId that = (RoleId) o;

        return (this.firstName == that.firstName || this.firstName.equals(that.firstName))
                && (this.lastName == that.lastName || this.lastName.equals(that.lastName));
    }

    public int hashCode()
    {
        int h1 = (firstName == null) ? 0 : firstName.hashCode();
        int h2 = (lastName == null) ? 0 : lastName.hashCode();
        return h1 + 31 * h2;
    }

}
