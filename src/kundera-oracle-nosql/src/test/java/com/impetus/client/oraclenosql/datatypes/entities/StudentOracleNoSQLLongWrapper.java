package com.impetus.client.oraclenosql.datatypes.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "StudentOracleNoSQLLongWrapper", schema = "KunderaTests@twikvstore")
public class StudentOracleNoSQLLongWrapper
{

    @Id
    private Long id;

    @Column(name = "AGE")
    private short age;

    @Column(name = "NAME")
    private String name;

    /**
     * @return the id
     */
    public Long getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(Long id)
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
