package com.impetus.kundera.metadata.model;

import java.lang.reflect.Field;

/**
 * Holds metadata for entity column
 *
 * @author animesh.kumar
 */
public final class Column
{

    /** name of the column. */
    private String name;

    /** column field. */
    private Field field;
    
    /** whether indexable. */
    private boolean isIndexable; // default is NOT indexable

    /**
     * Instantiates a new column.
     *
     * @param name
     *            the name
     * @param field
     *            the field
     */
    public Column(String name, Field field)
    {
        this.name = name;
        this.field = field;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Gets the field.
     *
     * @return the field
     */
    public Field getField()
    {
        return field;
    }

    /**
     * @return the isIndexable
     */
    public boolean isIndexable()
    {
        return isIndexable;
    }

    /**
     * @param isIndexable the isIndexable to set
     */
    public void setIndexable(boolean isIndexable)
    {
        this.isIndexable = isIndexable;
    }   

}