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
package com.impetus.kundera.metadata.entities.bi;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 * 
 */
@Entity
@Table(name = "asso_table", schema = "testSchema@keyspace")
public class AssociationBiEntity
{

    @Id
    @Column(name = "Association_ID")
    private String assoRowKey;

    @Column(name = "ADDRESS")
    private String address;

    @Column(name = "AGE")
    private int age;

    @OneToOne(mappedBy = "association")
    private OToOOwnerBiEntity owner;

    /**
     * @return the rowKey
     */
    public String getRowKey()
    {
        return assoRowKey;
    }

    /**
     * @param rowKey
     *            the rowKey to set
     */
    public void setRowKey(String rowKey)
    {
        this.assoRowKey = rowKey;
    }

    /**
     * @return the address
     */
    public String getAddress()
    {
        return address;
    }

    /**
     * @param address
     *            the address to set
     */
    public void setAddress(String address)
    {
        this.address = address;
    }

    /**
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * @return the assoRowKey
     */
    public String getAssoRowKey()
    {
        return assoRowKey;
    }

    /**
     * @param assoRowKey
     *            the assoRowKey to set
     */
    public void setAssoRowKey(String assoRowKey)
    {
        this.assoRowKey = assoRowKey;
    }

    /**
     * @return the owner
     */
    public OToOOwnerBiEntity getOwner()
    {
        return owner;
    }

    /**
     * @param owner
     *            the owner to set
     */
    public void setOwner(OToOOwnerBiEntity owner)
    {
        this.owner = owner;
    }

}
