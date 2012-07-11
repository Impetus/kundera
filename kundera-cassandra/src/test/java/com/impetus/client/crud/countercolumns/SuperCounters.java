package com.impetus.client.crud.countercolumns;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "SuperCounters", schema = "KunderaCounterColumn@CassandraCounterTest")
public class SuperCounters
{

    @Id
    @Column(name = "ID")
    private String id;

    @Column(name = "COUNTER")
    private int counter;

    @Embedded
    private SubCounter subCounter;

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
     * @return the counter
     */
    public int getCounter()
    {
        return counter;
    }

    /**
     * @param counter
     *            the counter to set
     */
    public void setCounter(int counter)
    {
        this.counter = counter;
    }

    /**
     * @return the subCounter
     */
    public SubCounter getSubCounter()
    {
        return subCounter;
    }

    /**
     * @param subCounter
     *            the subCounter to set
     */
    public void setSubCounter(SubCounter subCounter)
    {
        this.subCounter = subCounter;
    }
}
