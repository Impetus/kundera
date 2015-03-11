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
 * @author Pragalbh Garg
 *
 */
@Table(name = "HBASE_TABLE", schema = "HBaseNew@secTableTest")
@SecondaryTables({ @SecondaryTable(name = "HBASE_SECONDARY_TABLE"), @SecondaryTable(name = "t_country") })
@Entity
public class HbaseSecondaryTableEntity
{
    @Id
    @Column(name = "OBJECT_ID")
    private String objectId;

    @Column(name = "NAME")
    private String name;

    @Column(name = "AGE", table = "HBASE_SECONDARY_TABLE")
    private int age;

    @Column(name = "Country", table = "t_country")
    private String country;

    @OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn(name = "ADDRESS_ID")
    private PersonSecondaryTableAddress address;

    // @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER,
    // mappedBy = "hbaseSec")
    @ElementCollection
    @CollectionTable(name = "embeddedEntities")
    private List<EmbeddedCollectionEntity> embeddedEntities;

    @Embedded
    private EmbeddedEntity embeddedEntity;

    public String getObjectId()
    {
        return objectId;
    }

    public void setObjectId(String objectId)
    {
        this.objectId = objectId;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void setCountry(String country)
    {
        this.country = country;
    }

    public String getCountry()
    {
        return this.country;
    }

    public int getAge()
    {
        return age;
    }

    public void setAge(int age)
    {
        this.age = age;
    }

    public EmbeddedEntity getEmbeddedEntity()
    {
        return embeddedEntity;
    }

    public void setEmbeddedEntity(EmbeddedEntity embeddedEntity)
    {
        this.embeddedEntity = embeddedEntity;
    }

    public List<EmbeddedCollectionEntity> getEmbeddedEntities()
    {
        return embeddedEntities;
    }

    public void setEmbeddedEntities(List<EmbeddedCollectionEntity> embeddedEntities)
    {
        this.embeddedEntities = embeddedEntities;
    }

    public PersonSecondaryTableAddress getAddress()
    {
        return address;
    }

    public void setAddress(PersonSecondaryTableAddress address)
    {
        this.address = address;
    }

}
