package com.impetus.kundera.configure.schema;

/**
 * Class IndexInfo holds the information about index for a column.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class IndexInfo
{
    /** The column name variable . */
    private String columnName;

    /** Maximum allowed value for this column */
    private Integer maxValue;

    /** Minimum allowed value for this column */
    private Integer minValue;

    /** The index type. */
    private String indexType;

    /** Index name value */
    private String indexName;

    public IndexInfo(String columnName, Integer maxValue, Integer minValue, String indexType, String indexName)
    {
        this.columnName = columnName;
        this.maxValue = maxValue;
        this.minValue = minValue;
        this.indexType = indexType;
        this.indexName = indexName;
    }

    public IndexInfo(String columnName)
    {
        this(columnName, null, null, null, columnName);
    }

    public String getColumnName()
    {
        return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public Integer getMaxValue()
    {
        return maxValue;
    }

    public void setMaxValue(Integer maxValue)
    {
        this.maxValue = maxValue;
    }

    public Integer getMinValue()
    {
        return minValue;
    }

    public void setMinValue(Integer minValue)
    {
        this.minValue = minValue;
    }

    public String getIndexType()
    {
        return indexType;
    }

    public void setIndexType(String indexType)
    {
        this.indexType = indexType;
    }

    /**
     * Equals method compare two object of columnInfo on the basis of their
     * name.
     * 
     * @param Object
     *            instance.
     * 
     * @return boolean value.
     */
    @Override
    public boolean equals(Object columnName)
    {
        // if column name matches then return true;
        return columnName != null ? this.columnName.equals(columnName.toString()) : false;
    }

    @Override
    /**
     * returns the string representation of object .
     * 
     */
    public String toString()
    {
        return columnName;
    }

    /**
     * 
     * @return index name
     */
    public String getIndexName()
    {
        return indexName;
    }

}
