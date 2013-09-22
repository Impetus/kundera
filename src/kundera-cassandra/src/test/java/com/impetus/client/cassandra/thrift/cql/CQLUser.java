package com.impetus.client.cassandra.thrift.cql;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "CQLUSER", schema = "CqlKeyspace@cassandra_cql")
@IndexCollection(columns = { @Index(name = "name"), @Index(name = "age") })
public class CQLUser
{
    @Id
    private int id;

    @Column
    private String name;

    @Column
    private int age;

    @Column
    private transient int salary;

    @Column
    private static String address;

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
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

    public int getSalary()
    {
        return salary;
    }

    public void setSalary(int salary)
    {
        this.salary = salary;
    }

    public static String getAddress()
    {
        return address;
    }

    public static void setAddress(String address)
    {
        CQLUser.address = address;
    }

}
