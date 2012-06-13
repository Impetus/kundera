/**
 * 
 */
package com.impetus.client.schemaManager;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Kuldeep Mishra
 * @version 1.0
 * 
 */
@Entity
@Table(name = "DOCTOR", schema = "KunderaExamplesTests1@secIdxCassandra")
public class Doctor
{
    @Id
    private String id;

    @Column
    private String name;

    @Column
    private long age;

    /**
     * @return the id
     */
    public String getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(String id)
    {
        this.id = id;
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

    /**
     * @return the age
     */
    public long getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(long age)
    {
        this.age = age;
    }

}
