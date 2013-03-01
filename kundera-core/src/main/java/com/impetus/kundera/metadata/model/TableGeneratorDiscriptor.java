package com.impetus.kundera.metadata.model;

import javax.persistence.TableGenerator;
import javax.persistence.UniqueConstraint;

public class TableGeneratorDiscriptor
{
    private static final String default_table_name = "kundera_sequences";

    private static final String default_pkColumn_name = "sequence_name";

    private static final String default_valueColumn_name = "sequence_value";

    private static final int default_allocation_size = 50;

    private static final int default_initial_value = 0;

    private String name;

    private String table;

    private String catalog;

    private String schema;

    private String pkColumnName;

    private String valueColumnName;

    private String pkColumnValue;

    private int initialValue;

    private int allocationSize;

    private UniqueConstraint[] uniqueConstraints;

    public TableGeneratorDiscriptor(TableGenerator tableGenerator, String defaultSchemaName, String defaultPkColumnValue)
    {
        this.name = tableGenerator.name();
        this.table = tableGenerator.table().isEmpty() ? default_table_name : tableGenerator.table();
        this.schema = tableGenerator.schema().isEmpty() ? defaultSchemaName : tableGenerator.schema();
        this.pkColumnName = tableGenerator.pkColumnName().isEmpty() ? default_pkColumn_name : tableGenerator
                .pkColumnName();
        this.valueColumnName = tableGenerator.valueColumnName().isEmpty() ? default_valueColumn_name : tableGenerator
                .valueColumnName();
        this.pkColumnValue = tableGenerator.pkColumnValue().isEmpty() ? defaultPkColumnValue : tableGenerator
                .pkColumnValue();
        this.initialValue = tableGenerator.initialValue();
        this.allocationSize = tableGenerator.allocationSize();
    }

    public TableGeneratorDiscriptor(String defaultSchemaName, String defaultPkColumnValue)
    {
        this.table = default_table_name;
        this.schema = defaultSchemaName;
        this.pkColumnName = default_pkColumn_name;
        this.valueColumnName = default_valueColumn_name;
        this.pkColumnValue = defaultPkColumnValue;
        this.initialValue = default_initial_value;
        this.allocationSize = default_allocation_size;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return the table
     */
    public String getTable()
    {
        return table;
    }

    /**
     * @return the catalog
     */
    public String getCatalog()
    {
        return catalog;
    }

    /**
     * @return the schema
     */
    public String getSchema()
    {
        return schema;
    }

    /**
     * @return the pkColumnName
     */
    public String getPkColumnName()
    {
        return pkColumnName;
    }

    /**
     * @return the valueColumnName
     */
    public String getValueColumnName()
    {
        return valueColumnName;
    }

    /**
     * @return the pkColumnValue
     */
    public String getPkColumnValue()
    {
        return pkColumnValue;
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
     * @return the uniqueConstraints
     */
    public UniqueConstraint[] getUniqueConstraints()
    {
        return uniqueConstraints;
    }

}
