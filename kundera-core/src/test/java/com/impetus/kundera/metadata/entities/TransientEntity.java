/**
 * 
 */
package com.impetus.kundera.metadata.entities;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "transient_entity_table", schema = "testSchema@keyspace")
public class TransientEntity
{

    @Id
    @Column(name = "TransientEntity_Id")
    private String assoRowKey;

    @Column(name = "ADDRESS")
    private String address;

    @Column(name = "AGE")
    private transient int age;

    @Embedded
    private EmbeddableTransientEntity embeddableTransientField;

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
     * @return the embeddableTransientEntity
     */
    public EmbeddableTransientEntity getEmbeddableTransientEntity()
    {
        return embeddableTransientField;
    }

    /**
     * @param embeddableTransientEntity
     *            the embeddableTransientEntity to set
     */
    public void setEmbeddableTransientEntity(EmbeddableTransientEntity embeddableTransientEntity)
    {
        this.embeddableTransientField = embeddableTransientEntity;
    }

}
