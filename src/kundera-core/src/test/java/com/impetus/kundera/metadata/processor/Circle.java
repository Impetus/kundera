package com.impetus.kundera.metadata.processor;

import javax.persistence.Column;
import javax.persistence.Entity;

@Entity
public class Circle extends Shape
{
    @Column(name = "radius")
    private int radius;

 
  
    public int getEngineId()
    {
        return radius;
    }

    public void setEngineId(int radius)
    {
        this.radius = radius;
    }

   

}
