/**
 * 
 */
package com.impetus.kundera.ycsb.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "user", schema = "0@kundera_redis_pu")
public class RedisUser
{

    @Id
    private String name;

    @Column
    private String age;

    @Column
    private String address;

    @Column
    private String lname;

    public RedisUser()
    {

    }

    public RedisUser(String name, String age, String add, String lname)
    {
        this.lname = lname;
        this.address = add;
        this.name = name;
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

    /**
     * @return the age
     */
    public String getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(String age)
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

    /**
     * @return the lname
     */
    public String getLname()
    {
        return lname;
    }

    /**
     * @param lname
     *            the lname to set
     */
    public void setLname(String lname)
    {
        this.lname = lname;
    }

}
