package com.impetus.client.crud.countercolumns;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class SubCounter
{

    @Column(name = "SUB_COUNTER")
    private long subCounter;

    /**
     * @return the subCounter
     */
    public long getSubCounter()
    {
        return subCounter;
    }

    /**
     * @param subCounter
     *            the subCounter to set
     */
    public void setSubCounter(long subCounter)
    {
        this.subCounter = subCounter;
    }
}
