package com.impetus.kundera.metadata.processor;

import javax.persistence.Column;
import javax.persistence.Entity;

@Entity
public class Circle extends Shape
{
    @Column(name = "radius")
    private int radius;

 
  
    public int getRadius()
    {
        return radius;
    }

    public void setRadius(int radius)
    {
        this.radius = radius;
    }

   

}
