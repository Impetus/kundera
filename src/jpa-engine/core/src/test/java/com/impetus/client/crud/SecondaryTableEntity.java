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
package com.impetus.client.crud;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.SecondaryTable;
import javax.persistence.Table;

/**
 * 
 * @author Kuldeep.Mishra
 * 
 */
@Table(name = "PRIMARY_TABLE")
@SecondaryTable(name = "SECONDARY_TABLE")
@Entity
public class SecondaryTableEntity
{
    @Id
    @Column(name = "OBJECT_ID")
    private String objectId;

    @Column(name = "NAME")
    private String name;

    @Column(name = "AGE", table = "SECONDARY_TABLE")
    private int age;

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

}
