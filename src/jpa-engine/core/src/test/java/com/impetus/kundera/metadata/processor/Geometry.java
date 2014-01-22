package com.impetus.kundera.metadata.processor;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToOne;



@Entity
public class Geometry
{
    @Id
    private String geoId;
    
    @Column(name = "name")
    private String name;
    
    @OneToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
  //  @JoinColumn(name = "shape_id")
    private Shape shape;

   
    public String getGeoId()
    {
        return geoId;
    }

    public void setGeoId(String geoId)
    {
        this.geoId = geoId;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }
    
    public Shape getShape()
    {
        return shape;
    }

    public void setShape(Shape shape)
    {
        this.shape = shape;
    }

}
