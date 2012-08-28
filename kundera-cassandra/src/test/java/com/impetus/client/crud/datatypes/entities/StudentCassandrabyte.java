package com.impetus.client.crud.datatypes.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "StudentCassandrabyte", schema = "KunderaCassandraDataType@CassandraDataTypeTest")
public class StudentCassandrabyte
{


    @Id
    private byte id;

    @Column(name = "AGE")
    private short age;

    @Column(name = "NAME")
    private String name;

    /**
     * @return the id
     */
    public byte getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(byte id)
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
