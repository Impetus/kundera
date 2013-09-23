package com.impetus.client.mongodb.schemamanager;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import com.impetus.kundera.gis.geometry.Point;
import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Embeddable
@IndexCollection(columns = { @Index(name = "currentLocation", type = "GEO2D", min = -100, max = 500),
        @Index(name = "previousLocation", type = "GEO2D", min = 100, max = 400) })
public class Location
{
    @Column(name = "CURRENT_LOCATION")
    private Point currentLocation;

    @Column(name = "PREVIOUS_LOCATION")
    private Point previousLocation;

    /**
     * @return the currentLocation
     */
    public Point getCurrentLocation()
    {
        return currentLocation;
    }

    /**
     * @param currentLocation
     *            the currentLocation to set
     */
    public void setCurrentLocation(Point currentLocation)
    {
        this.currentLocation = currentLocation;
    }

    /**
     * @return the previousLocation
     */
    public Point getPreviousLocation()
    {
        return previousLocation;
    }

    /**
     * @param previousLocation
     *            the previousLocation to set
     */
    public void setPreviousLocation(Point previousLocation)
    {
        this.previousLocation = previousLocation;
    }
}
