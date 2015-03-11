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
package com.impetus.client.hbase.secondarytable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Pragalbh Garg
 * 
 */
@Entity
@Table(name = "SEC_TABLE", schema = "HBaseNew@secTableTest")
public class PersonSecondaryTableAddress
{
    @Id
    @Column(name = "ADDRESS_ID")
    private Double addressId;

    public PersonSecondaryTableAddress()
    {

    }

    public PersonSecondaryTableAddress(Double addressId)
    {
        this.addressId = addressId;
    }

    @Column(name = "address")
    private String address;

    public String getAddress()
    {
        return address;
    }

    public void setAddress(String address)
    {
        this.address = address;
    }

    public Double getAddressId()
    {
        return addressId;
    }

}
