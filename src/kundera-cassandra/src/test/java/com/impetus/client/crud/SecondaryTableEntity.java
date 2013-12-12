package com.impetus.client.crud;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.SecondaryTable;
import javax.persistence.Table;

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
