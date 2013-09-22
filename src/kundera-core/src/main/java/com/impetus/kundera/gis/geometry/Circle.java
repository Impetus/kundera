/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.gis.geometry;

import com.impetus.kundera.gis.SurfaceType;

/**
 * Geometric class representing circle shape
 * 
 * @author amresh.singh
 */
public class Circle
{
    private Coordinate centre;

    private double radius;

    /** Surface type on which this circle is based */
    private SurfaceType surfaceType = SurfaceType.FLAT;

    public Circle(double x, double y, double r)
    {
        setCentre(new Coordinate(x, y));
        setRadius(r);
    }

    public Circle(Coordinate centre, double radius)
    {
        this.centre = centre;
        this.radius = radius;
    }

    /**
     * @return the centre
     */
    public Coordinate getCentre()
    {
        return centre;
    }

    /**
     * @param centre
     *            the centre to set
     */
    public void setCentre(Coordinate centre)
    {
        this.centre = centre;
    }

    /**
     * @return the radius
     */
    public double getRadius()
    {
        return radius;
    }

    /**
     * @param radius
     *            the radius to set
     */
    public void setRadius(double radius)
    {
        this.radius = radius;
    }

    /**
     * @return the surfaceType
     */
    public SurfaceType getSurfaceType()
    {
        return surfaceType;
    }

    /**
     * @param surfaceType
     *            the surfaceType to set
     */
    public void setSurfaceType(SurfaceType surfaceType)
    {
        this.surfaceType = surfaceType;
    }

}
