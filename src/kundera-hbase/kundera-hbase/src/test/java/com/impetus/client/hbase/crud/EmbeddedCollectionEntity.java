package com.impetus.client.hbase.crud;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class EmbeddedCollectionEntity
{
    @Column(name = "COLLECTION_ID")
    private String collectionId;

    @Column(name = "COLLECTION_NAME")
    private String collectionName;

    public String getCollectionId()
    {
        return collectionId;
    }

    public void setCollectionId(String collectionId)
    {
        this.collectionId = collectionId;
    }

    public String getCollectionName()
    {
        return collectionName;
    }

    public void setCollectionName(String collectionName)
    {
        this.collectionName = collectionName;
    }

}
