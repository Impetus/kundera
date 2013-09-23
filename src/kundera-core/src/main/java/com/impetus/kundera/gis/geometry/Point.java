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
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Geometric Point implementation
 * 
 * @author amresh.singh
 */
public class Point extends com.vividsolutions.jts.geom.Point
{
    /** Surface type on which this point is based */
    private SurfaceType surfaceType = SurfaceType.FLAT;

    public Point(com.vividsolutions.jts.geom.Point point)
    {
        super(point.getCoordinate(), point.getPrecisionModel(), point.getSRID());
    }

    public Point(double x, double y)
    {
        super(new Coordinate(x, y), new PrecisionModel(), 0);
    }

    /**
     * @param coordinates
     * @param factory
     */
    public Point(CoordinateSequence coordinates, GeometryFactory factory)
    {
        super(coordinates, factory);
    }

    /**
     * @param coordinate
     * @param precisionModel
     * @param SRID
     * @deprecated
     */
    public Point(Coordinate coordinate, PrecisionModel precisionModel, int SRID)
    {
        super(coordinate, precisionModel, SRID);

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
