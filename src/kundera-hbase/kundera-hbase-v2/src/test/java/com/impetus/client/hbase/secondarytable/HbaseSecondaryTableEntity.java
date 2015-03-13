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

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.SecondaryTable;
import javax.persistence.SecondaryTables;
import javax.persistence.Table;

/**
 * The Class HbaseSecondaryTableEntity.
 * 
 * @author Pragalbh Garg
 */
@Table(name = "HBASE_TABLE", schema = "HBaseNew@secTableTest")
@SecondaryTables({ @SecondaryTable(name = "HBASE_SECONDARY_TABLE"), @SecondaryTable(name = "t_country") })
@Entity
public class HbaseSecondaryTableEntity
{

    /** The object id. */
    @Id
    @Column(name = "OBJECT_ID")
    private String objectId;

    /** The name. */
    @Column(name = "NAME")
    private String name;

    /** The age. */
    @Column(name = "AGE", table = "HBASE_SECONDARY_TABLE")
    private int age;

    /** The country. */
    @Column(name = "Country", table = "t_country")
    private String country;

    /** The address. */
    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn(name = "ADDRESS_ID")
    private PersonSecondaryTableAddress address;

    /** The embedded entities. */
    @ElementCollection
    @CollectionTable(name = "embeddedEntities")
    private List<EmbeddedCollectionEntity> embeddedEntities;

    /** The embedded entity. */
    @Embedded
    private EmbeddedEntity embeddedEntity;

    /**
     * Gets the object id.
     * 
     * @return the object id
     */
    public String getObjectId()
    {
        return objectId;
    }

    /**
     * Sets the object id.
     * 
     * @param objectId
     *            the new object id
     */
    public void setObjectId(String objectId)
    {
        this.objectId = objectId;
    }

    /**
     * Gets the name.
     * 
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     * 
     * @param name
     *            the new name
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Sets the country.
     * 
     * @param country
     *            the new country
     */
    public void setCountry(String country)
    {
        this.country = country;
    }

    /**
     * Gets the country.
     * 
     * @return the country
     */
    public String getCountry()
    {
        return this.country;
    }

    /**
     * Gets the age.
     * 
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     * 
     * @param age
     *            the new age
     */
    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * Gets the embedded entity.
     * 
     * @return the embedded entity
     */
    public EmbeddedEntity getEmbeddedEntity()
    {
        return embeddedEntity;
    }

    /**
     * Sets the embedded entity.
     * 
     * @param embeddedEntity
     *            the new embedded entity
     */
    public void setEmbeddedEntity(EmbeddedEntity embeddedEntity)
    {
        this.embeddedEntity = embeddedEntity;
    }

    /**
     * Gets the embedded entities.
     * 
     * @return the embedded entities
     */
    public List<EmbeddedCollectionEntity> getEmbeddedEntities()
    {
        return embeddedEntities;
    }

    /**
     * Sets the embedded entities.
     * 
     * @param embeddedEntities
     *            the new embedded entities
     */
    public void setEmbeddedEntities(List<EmbeddedCollectionEntity> embeddedEntities)
    {
        this.embeddedEntities = embeddedEntities;
    }

    /**
     * Gets the address.
     * 
     * @return the address
     */
    public PersonSecondaryTableAddress getAddress()
    {
        return address;
    }

    /**
     * Sets the address.
     * 
     * @param address
     *            the new address
     */
    public void setAddress(PersonSecondaryTableAddress address)
    {
        this.address = address;
    }

}
