/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.kundera.client.crud.associations;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * @author Pragalbh Garg
 *
 */
@Entity
@Table(name = "mobile_handset")
public class MobileHandset
{

    @Id
    private String id;
    
    @Column(name="mob_name")
    private String name;
    
    @ManyToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "os")
    private MobileOperatingSystem os;
    
    @ManyToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name="manufacturer")
    private MobileManufacturer manufacturer;

    public MobileManufacturer getManufacturer()
    {
        return manufacturer;
    }

    public void setManufacturer(MobileManufacturer manufacturer)
    {
        this.manufacturer = manufacturer;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public MobileOperatingSystem getOs()
    {
        return os;
    }

    public void setOs(MobileOperatingSystem os)
    {
        this.os = os;
    }
    
    
}
