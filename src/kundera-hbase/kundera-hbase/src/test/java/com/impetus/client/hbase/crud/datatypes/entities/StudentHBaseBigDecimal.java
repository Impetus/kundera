package com.impetus.client.hbase.crud.datatypes.entities;

import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "StudentHBaseBigDecimal", schema = "KunderaHbaseDataType@HbaseDataTypeTest")
public class StudentHBaseBigDecimal
{

    @Id
    private BigDecimal id;

    @Column(name = "AGE")
    private short age;

    @Column(name = "NAME")
    private String name;

    /**
     * @return the id
     */
    public BigDecimal getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(BigDecimal id)
    {
        this.id = id;
    }

    /**
     * @return the age
     */
    public short getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(short age)
    {
        this.age = age;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

}
