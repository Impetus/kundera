package com.impetus.client.schemamanager.entites;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "InvalidCounterColumnEntity", schema = "KunderaCounterColumn@cassandraProperties")
public class InvalidCounterColumnEntity
{

    @Id
    private int id;

    @Column
    private long counter;

    @Column
    private String lCounter;

    /**
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(int id)
    {
        this.id = id;
    }

    /**
     * @return the counter
     */
    public long getCounter()
    {
        return counter;
    }

    /**
     * @param counter
     *            the counter to set
     */
    public void setCounter(long counter)
    {
        this.counter = counter;
    }

    /**
     * @return the lCounter
     */
    public String getlCounter()
    {
        return lCounter;
    }

    /**
     * @param lCounter
     *            the lCounter to set
     */
    public void setlCounter(String lCounter)
    {
        this.lCounter = lCounter;
    }
}
