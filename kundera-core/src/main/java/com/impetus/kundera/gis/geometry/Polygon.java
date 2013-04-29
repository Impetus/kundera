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

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

/**
 * Geometric Polygon implementation
 * 
 * @author amresh.singh
 */
public class Polygon extends com.vividsolutions.jts.geom.Polygon
{

    /**
     * @param shell
     * @param holes
     * @param factory
     */
    public Polygon(LinearRing shell, LinearRing[] holes, GeometryFactory factory)
    {
        super(shell, holes, factory);
    }

    @Override
    public Coordinate[] getCoordinates()
    {
        com.vividsolutions.jts.geom.Coordinate[] coordinates = super.getCoordinates();

        Coordinate[] cs = new Coordinate[coordinates.length];

        int count = 0;
        for (com.vividsolutions.jts.geom.Coordinate c : coordinates)
        {
            cs[count++] = new Coordinate(c);
        }

        return cs;
    }
}
