package com.impetus.kundera.metadata.model;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds metadata for embedded column in entity
 *
 * @author animesh.kumar
 */
public final class EmbeddedColumn
{

    /** The name. */
    private String name;

    /** Super column field. */
    private Field field;

    /** The columns. */
    private List<Column> columns;

    /**
     * Instantiates a new super column.
     *
     * @param name the name
     * @param f the f
     */
    public EmbeddedColumn(String name, Field f)
    {
        this.name = name;
        this.field = f;
        columns = new ArrayList<Column>();
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
     * Sets the name.
     *
     * @param name the name to set
     */
    public void setName(String name)
    {
        this.name = name;
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
     * Sets the field.
     *
     * @param field the field to set
     */
    public void setField(Field field)
    {
        this.field = field;
    }

    /**
     * Gets the columns.
     *
     * @return the columns
     */
    public List<Column> getColumns()
    {
        return columns;
    }    
    
    

    /**
     * Adds the column.
     *
     * @param name
     *            the name
     * @param field
     *            the field
     */
    public void addColumn(String name, Field field)
    {
        columns.add(new Column(name, field));
    }
}