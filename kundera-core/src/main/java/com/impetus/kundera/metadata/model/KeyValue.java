package com.impetus.kundera.metadata.model;

import javax.persistence.GenerationType;

public class KeyValue
{
    private boolean isAssigned;

    private GenerationType strategy;

    private TableGeneratorDiscriptor tableDiscriptor;

    private SequenceGeneratorDiscriptor sequenceDiscriptor;

    /**
     * @return the isAssigned
     */
    public boolean isAssigned()
    {
        return isAssigned;
    }

    /**
     * @param isAssigned
     *            the isAssigned to set
     */
    public void setAssigned(boolean isAssigned)
    {
        this.isAssigned = isAssigned;
    }

    /**
     * @return the strategy
     */
    public GenerationType getStrategy()
    {
        return strategy;
    }

    /**
     * @param strategy
     *            the strategy to set
     */
    public void setStrategy(GenerationType strategy)
    {
        this.strategy = strategy;
    }

    /**
     * @return the tableDiscriptor
     */
    public TableGeneratorDiscriptor getTableDiscriptor()
    {
        return tableDiscriptor;
    }

    /**
     * @param tableDiscriptor
     *            the tableDiscriptor to set
     */
    public void setTableDiscriptor(TableGeneratorDiscriptor tableDiscriptor)
    {
        this.tableDiscriptor = tableDiscriptor;
    }

    /**
     * @return the sequenceDiscriptor
     */
    public SequenceGeneratorDiscriptor getSequenceDiscriptor()
    {
        return sequenceDiscriptor;
    }

    /**
     * @param sequenceDiscriptor
     *            the sequenceDiscriptor to set
     */
    public void setSequenceDiscriptor(SequenceGeneratorDiscriptor sequenceDiscriptor)
    {
        this.sequenceDiscriptor = sequenceDiscriptor;
    }

}
