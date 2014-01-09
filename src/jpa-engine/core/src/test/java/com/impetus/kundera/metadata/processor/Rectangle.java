package com.impetus.kundera.metadata.processor;

import javax.persistence.Column;
import javax.persistence.Entity;


@Entity
public class Rectangle extends Polygon
{
   
    @Column(name = "length")
    private int length;

    @Column(name = "breadth")
    private int breadth;
    
     
    public int getLength()
    {
        return length;
    }

    public void setLength(int length)
    {
        this.length = length;
    }

    public int getBreadth()
    {
        return breadth;
    }

    public void setBreadth(int breadth)
    {
        this.breadth = breadth;
    }
    
    

}
