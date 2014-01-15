/**
 * 
 */
package com.impetus.kundera.metadata.entities;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * @author Kuldeep Mishra
 * 
 */
@Embeddable
public class EmbeddableTransientEntity
{

    @Column(name = "field")
    private Float embeddedField;

    @Column(name = "name")
    private transient String embeddedName;

    /**
     * @return the field
     */
    public Float getField()
    {
        return embeddedField;
    }

    /**
     * @param field
     *            the field to set
     */
    public void setField(Float field)
    {
        this.embeddedField = field;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return embeddedName;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.embeddedName = name;
    }
}
