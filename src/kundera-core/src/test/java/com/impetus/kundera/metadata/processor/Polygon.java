package com.impetus.kundera.metadata.processor;

import javax.persistence.Column;
import javax.persistence.Entity;

@Entity
public class Polygon extends Shape
{
    
    @Column(name = "sides")
    private int sides;

    @Column(name = "type")
    private String type;

  
    public int getSides()
    {
        return sides;
    }

    public void setSides(int sides)
    {
        this.sides = sides;
    }

    public String getType()
    {
        return type;
    }

    public void setgetType(String type)
    {
        this.type = type;
    }

}
