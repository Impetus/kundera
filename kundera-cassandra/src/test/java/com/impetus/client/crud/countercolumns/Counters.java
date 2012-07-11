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
    private int counter1;

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
    public int getCounter1()
    {
        return counter1;
    }

    /**
     * @param counter1
     *            the counter1 to set
     */
    public void setCounter1(int counter1)
    {
        this.counter1 = counter1;
    }
}
