/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql.entities;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * Office Embeddable object
 * 
 * @author amresh.singh
 */
@Embeddable
public class Office
{
    @Column private int officeId;

    @Column private String companyName;

    @Column private String location;
    
    public Office()
    {    
        
    }

    public Office(int officeId, String companyName, String location)
    {
        this.officeId = officeId;
        this.companyName = companyName;
        this.location = location;
    }

    /**
     * @return the officeId
     */
    public int getOfficeId()
    {
        return officeId;
    }

    /**
     * @param officeId
     *            the officeId to set
     */
    public void setOfficeId(int officeId)
    {
        this.officeId = officeId;
    }

    /**
     * @return the companyName
     */
    public String getCompanyName()
    {
        return companyName;
    }

    /**
     * @param companyName
     *            the companyName to set
     */
    public void setCompanyName(String companyName)
    {
        this.companyName = companyName;
    }

    /**
     * @return the location
     */
    public String getLocation()
    {
        return location;
    }

    /**
     * @param location
     *            the location to set
     */
    public void setLocation(String location)
    {
        this.location = location;
    }

}
