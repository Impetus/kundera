package com.impetus.client.cassandra.config;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.client.twitter.entities.PersonalDetailCassandra;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "CassandraDefaultSuperUser", schema = "KunderaCassandraXmlTest@CassandraXmlPropertyTest")
public class CassandraDefaultSuperUser
{
    @Id
    private String name;

    @Column
    private int age;

    @Column
    private String address;

    @Embedded
    private PersonalDetailCassandra personalDetailCassandra;

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

    public PersonalDetailCassandra getPersonalDetailCassandra()
    {
        return personalDetailCassandra;
    }

    public void setPersonalDetailCassandra(PersonalDetailCassandra personalDetailCassandra)
    {
        this.personalDetailCassandra = personalDetailCassandra;
    }

}
