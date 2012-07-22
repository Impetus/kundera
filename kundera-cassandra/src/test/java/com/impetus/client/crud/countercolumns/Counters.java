package com.impetus.client.crud.countercolumns;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "counters", schema = "KunderaCounterColumn@CassandraCounterTest")
public class Counters
{
    @Id
    private String id;

    @Column
    private int counter;

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
     * @return the counter1
     */
    public int getCounter()
    {
        return counter;
    }

    /**
     * @param counter1
     *            the counter1 to set
     */
    public void setCounter(int counter)
    {
        this.counter = counter;
    }
}
