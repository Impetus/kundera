package com.impetus.kundera.metadata.processor;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Embeddable
@IndexCollection(columns={@Index(name="tyreId")})
public class CarTyre
{

    @Column(name = "TYRE_ID")
    private String tyreId;

    @Column(name = "TYRE_TYPE")
    private String tyreType;

  
    public String getTyreId()
    {
        return tyreId;
    }

    public void setTyreId(String tyreId)
    {
        this.tyreId = tyreId;
    }

    public String getTyreType()
    {
        return tyreType;
    }

    public void setTyreType(String tyreType)
    {
        this.tyreType = tyreType;
    }

   


}
