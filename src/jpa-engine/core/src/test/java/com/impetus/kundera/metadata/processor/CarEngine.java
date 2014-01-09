package com.impetus.kundera.metadata.processor;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Embeddable
@IndexCollection(columns={@Index(name="engineId")})
public class CarEngine
{

    @Column(name = "ENGINE_ID")
    private String engineId;

    @Column(name = "SERIES")
    private String series;

  
    public String getEngineId()
    {
        return engineId;
    }

    public void setEngineId(String engineId)
    {
        this.engineId = engineId;
    }

    public String getSeries()
    {
        return series;
    }

    public void setSeries(String series)
    {
        this.series = series;
    }

   


}
