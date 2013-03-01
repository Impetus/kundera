package com.impetus.kundera.metadata.model;

import javax.persistence.SequenceGenerator;

public class SequenceGeneratorDiscriptor
{
    private static final int default_initial_value = 1;

    private static final String default_sequence_name = "sequence_name";

    private static final int default_allocation_size = 50;

    private String name;

    private int initialValue;

    private int allocationSize;

    private String sequenceName;

    private String schemaName;

    private String catalog;

    public SequenceGeneratorDiscriptor(SequenceGenerator sequenceGenerator, String defaultSchemaName)
    {
        this.name = sequenceGenerator.name();
        this.initialValue = sequenceGenerator.initialValue();
        this.allocationSize = sequenceGenerator.allocationSize();
        this.sequenceName = sequenceGenerator.sequenceName().isEmpty() ? default_sequence_name : sequenceGenerator
                .sequenceName();
        this.schemaName = sequenceGenerator.schema().isEmpty() ? defaultSchemaName : sequenceGenerator.schema();
    }

    public SequenceGeneratorDiscriptor(String defaultSchemaName)
    {
        this.initialValue = default_initial_value;
        this.allocationSize = default_allocation_size;
        this.sequenceName = default_sequence_name;
        this.schemaName = defaultSchemaName;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return the initialValue
     */
    public int getInitialValue()
    {
        return initialValue;
    }

    /**
     * @return the allocationSize
     */
    public int getAllocationSize()
    {
        return allocationSize;
    }

    /**
     * @return the sequenceName
     */
    public String getSequenceName()
    {
        return sequenceName;
    }

    /**
     * @return the schemaName
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * @return the catalog
     */
    public String getCatalog()
    {
        return catalog;
    }

}
